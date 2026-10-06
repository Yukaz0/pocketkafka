// Package server implements the TCP network listener and request dispatcher for
// the broker. It reads length-prefixed Kafka frames and routes them to the
// handler package.
package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/audit"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/credential"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/ratelimit"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// Server accepts TCP connections and dispatches Kafka requests.
type Server struct {
	cfg          *config.Config
	handler      *handler.Handler
	bindPorts    map[net.Listener]int32 // port bind per listener (untuk advertise per-listener)
	maxReqSize   int64
	preAuthSize  int64
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration
	listeners    []net.Listener
	tlsConf      *tls.Config
	wg           sync.WaitGroup
	closeCh      chan struct{}

	// saslLimiter refuses *connections* from a source that keeps failing SASL
	// (keyed by address, not by address:port, so reconnecting does not reset
	// it), and maxSASLFailures closes a connection that never gets it right.
	saslLimiter     *ratelimit.Limiter
	maxSASLFailures int

	// connSem bounds the number of concurrently served connections to
	// network.max_connections. A connection that cannot take a slot is closed
	// immediately and counted in rejections.
	connSem    chan struct{}
	rejections atomic.Int64

	// conns tracks live connections so Close can tear them down instead of
	// waiting forever for clients that never disconnect. closing guards against
	// a connection being registered after Close has swept the set.
	connMu  sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

// connState carries per-connection authentication state.
type connState struct {
	authed        bool
	principal     string // authenticated username, empty while unauthenticated
	mechanism     string // "PLAIN" or "SCRAM-SHA-256"/"SCRAM-SHA-512"
	scram         *scramSession
	scramUsername string
	// tls records whether this connection is encrypted; SASL/PLAIN is refused
	// without it unless the operator opts out.
	tls bool
	// saslFailures counts rejected authentication attempts on this connection.
	saslFailures int
}

// maxSASLFailuresPerConn is how many rejected SASL attempts a single connection
// may make before it is closed.
const maxSASLFailuresPerConn = 3

// New creates a Server that routes requests to h.
func New(cfg *config.Config, h *handler.Handler) *Server {
	maxConns := cfg.Network.MaxConnections
	if maxConns <= 0 {
		// Config validation rejects this for real configs; keep the server
		// itself fail-closed rather than treating 0 as "unlimited".
		maxConns = 1
	}
	return &Server{
		cfg:             cfg,
		handler:         h,
		bindPorts:       make(map[net.Listener]int32),
		maxReqSize:      cfg.Network.MaxRequestSizeBytes,
		preAuthSize:     cfg.Network.PreAuthMaxRequestBytes,
		readTimeout:     time.Duration(cfg.Network.ReadTimeoutMs) * time.Millisecond,
		writeTimeout:    time.Duration(cfg.Network.WriteTimeoutMs) * time.Millisecond,
		idleTimeout:     time.Duration(cfg.Network.IdleTimeoutMs) * time.Millisecond,
		closeCh:         make(chan struct{}),
		connSem:         make(chan struct{}, maxConns),
		conns:           make(map[net.Conn]struct{}),
		saslLimiter:     ratelimit.New(3, time.Second, 30*time.Second),
		maxSASLFailures: maxSASLFailuresPerConn,
	}
}

// RejectedConnections reports how many connections were refused because the
// max_connections limit was full.
func (s *Server) RejectedConnections() int64 { return s.rejections.Load() }

// Start binds all configured listeners and begins accepting connections.
func (s *Server) Start() error {
	seen := map[string]bool{}
	for _, addr := range s.cfg.Listeners {
		if seen[addr] {
			continue
		}
		seen[addr] = true
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			s.Close()
			return fmt.Errorf("listen on %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, ln)
		if tcAddr, ok := ln.Addr().(*net.TCPAddr); ok {
			s.bindPorts[ln] = int32(tcAddr.Port)
		}
		log.Printf("pocketkafka listening on %s", addr)
	}

	// Optional TLS (SSL) listener. security.tls.cert_file/key_file are shared
	// with the HTTP surfaces and MQTT, so one certificate covers the broker.
	var tlsPort int32
	if s.cfg.Security.TLS.Enabled {
		tlsConf, err := s.cfg.Security.TLS.ServerConfig()
		if err != nil {
			s.Close()
			return err
		}
		s.tlsConf = tlsConf
		ln, err := net.Listen("tcp", s.cfg.Security.TLS.Listen)
		if err != nil {
			s.Close()
			return fmt.Errorf("listen TLS on %s: %w", s.cfg.Security.TLS.Listen, err)
		}
		s.listeners = append(s.listeners, ln)
		if tcAddr, ok := ln.Addr().(*net.TCPAddr); ok {
			tlsPort = int32(tcAddr.Port)
		}
		log.Printf("pocketkafka TLS listening on %s (%s+, mTLS=%v)",
			s.cfg.Security.TLS.Listen, tlsVersionLabel(s.cfg.Security.TLS.MinVersion), s.cfg.Security.TLS.ClientCAFile != "")
	}

	// Register advertised address per bind port: koneksi yang masuk lewat
	// listener N dijawab metadata dengan advertised listener pasangannya,
	// bukan satu alamat global (pencegah reconnect-storm klien docker yang
	// diberi localhost padahal jalurnya jaringan internal).
	for name, addr := range s.cfg.Listeners {
		_, bindPort, ok := splitPort(addr)
		if !ok {
			continue
		}
		adv, ok := s.cfg.AdvertisedListeners[name]
		if !ok || adv == "" {
			adv = advertisedPrimary(s.cfg)
		}
		advHost, advPort, ok := splitPort(adv)
		if !ok {
			continue
		}
		s.handler.SetAdvertisedForPort(bindPort, advHost, advPort)
	}

	// A TLS client must never be told to reconnect to a plaintext listener: it
	// would then send SASL/PLAIN and its records in the clear (a downgrade).
	// The SSL listener gets its own advertised entry, or the primary advertised
	// host paired with the TLS port.
	if tlsPort != 0 {
		adv, ok := s.cfg.AdvertisedListeners["ssl"]
		if !ok || adv == "" {
			primaryHost, _, _ := splitPort(advertisedPrimary(s.cfg))
			if host, _, err := net.SplitHostPort(s.cfg.Security.TLS.Listen); err == nil &&
				host != "" && host != "0.0.0.0" && host != "::" && !config.IsLoopbackHost(host) {
				// A concrete bind host (e.g. 10.0.0.5) is what this broker is
				// reachable at from the network the client arrived on.
				primaryHost = host
			}
			adv = net.JoinHostPort(primaryHost, strconv.Itoa(int(tlsPort)))
		}
		if advHost, advPort, ok := splitPort(adv); ok {
			s.handler.SetAdvertisedForPort(tlsPort, advHost, advPort)
		}
	}

	for _, ln := range s.listeners {
		s.wg.Add(1)
		go s.acceptLoop(ln)
	}
	return nil
}

// splitPort returns the numeric port of a host:port address.
func splitPort(addr string) (string, int32, bool) {
	host, portStr, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return "", 0, false
	}
	port, err := strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil || port < 1 || port > 65535 {
		return "", 0, false
	}
	return host, int32(port), true
}

// tlsVersionLabel names the minimum TLS version for the startup log.
func tlsVersionLabel(min string) string {
	if strings.TrimSpace(min) == "1.3" {
		return "TLS1.3"
	}
	return "TLS1.2"
}

// advertisedPrimary returns the primary advertised address (same rule as the
// cmd/server helpers, duplicated here to keep the server package decoupled).
func advertisedPrimary(cfg *config.Config) string {
	if a, ok := cfg.AdvertisedListeners["plain"]; ok && a != "" {
		return a
	}
	// Sorted fallback: ranging the map would make the advertised address differ
	// between restarts.
	names := make([]string, 0, len(cfg.AdvertisedListeners))
	for name := range cfg.AdvertisedListeners {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if v := cfg.AdvertisedListeners[name]; v != "" {
			return v
		}
	}
	return "localhost:9092"
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return
		}
		if s.tlsConf != nil {
			conn = tls.Server(conn, s.tlsConf)
		}
		select {
		case s.connSem <- struct{}{}:
		default:
			// Connection slots are exhausted: refuse fast instead of queueing
			// an unbounded number of goroutines.
			s.rejections.Add(1)
			log.Printf("rejecting connection from %s: max_connections %d reached", conn.RemoteAddr(), cap(s.connSem))
			conn.Close()
			continue
		}
		if !s.registerConn(conn) {
			// Close is already running; do not start a worker that Close would
			// not be able to tear down.
			conn.Close()
			<-s.connSem
			return
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// registerConn adds conn to the live-connection set. It returns false when the
// server is shutting down, in which case the caller must close conn and release
// its slot.
func (s *Server) registerConn(conn net.Conn) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closing {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

// releaseConn removes conn from the live set, closes it, and frees its slot.
func (s *Server) releaseConn(conn net.Conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
	conn.Close()
	<-s.connSem
}

// readFrame reads one request frame, applying the idle deadline while waiting
// for a frame to start and the read deadline once bytes have arrived. This
// bounds both a stalled connect-and-say-nothing client and a client that sends
// a frame header then stalls mid-body. maxSize bounds the frame: callers pass
// the pre-authentication cap until the connection has authenticated.
func (s *Server) readFrame(conn net.Conn, maxSize int64) (*protocol.RequestHeader, []byte, error) {
	if s.idleTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			return nil, nil, err
		}
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, nil, err
	}
	if s.readTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(s.readTimeout)); err != nil {
			return nil, nil, err
		}
	}
	// Replay the length prefix we already consumed, then let the protocol
	// decoder read the body from the connection.
	frame := io.MultiReader(bytes.NewReader(lenBuf[:]), conn)
	return protocol.ReadRequestFrame(frame, maxSize)
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer s.releaseConn(conn)

	tcp, ok := conn.(*net.TCPConn)
	if ok {
		tcp.SetKeepAlive(true)
		tcp.SetNoDelay(true)
		if n := s.cfg.Network.ReadBufferBytes; n > 0 {
			tcp.SetReadBuffer(n)
		}
		if n := s.cfg.Network.WriteBufferBytes; n > 0 {
			tcp.SetWriteBuffer(n)
		}
	}

	// A source that is in backoff is refused at the door: the per-connection
	// attempt cap alone is useless, because a reconnect resets it. The key is
	// the source address without its ephemeral port, which is what makes the
	// failures aggregate across connections.
	if s.cfg.Security.Enabled {
		key := remoteHost(conn)
		if ok, retry := s.saslLimiter.Allow(key); !ok {
			audit.Log(audit.Event{
				Actor:      audit.Anonymous,
				Action:     "auth.sasl",
				Resource:   "connect",
				Result:     audit.ResultDeny,
				Detail:     fmt.Sprintf("source in backoff for %s", retry.Round(time.Millisecond)),
				RemoteAddr: remoteAddr(conn),
			})
			return
		}
	}

	// When SASL is enabled, a connection must authenticate before issuing any
	// non-SASL request.
	st := &connState{authed: !s.cfg.Security.Enabled}
	// Mutual TLS authenticates the connection by itself: the handshake already
	// required a certificate signed by security.tls.client_ca_file, so the
	// verified subject is this connection's principal and no SASL exchange is
	// needed.
	if principal := s.clientCertPrincipal(conn); principal != "" {
		st.authed = true
		st.principal = principal
		st.mechanism = "mTLS"
	}
	st.tls = isTLSConn(conn)

	for {
		// Before authentication the frame cap is the small pre-auth bound: an
		// anonymous client must not be able to make the broker allocate the
		// full 100 MB it is willing to accept from an authenticated producer.
		maxSize := s.maxReqSize
		if !st.authed {
			maxSize = s.preAuthSize
		}
		hdr, body, err := s.readFrame(conn, maxSize)
		if err != nil {
			return
		}

		if !st.authed {
			switch hdr.ApiKey {
			case protocol.APKApiVersions:
				// Allowed pre-auth; falls through to the normal handler.
			case protocol.APKSaslHandshake:
				resp, err := s.handleSASLHandshake(st, hdr, body)
				if err != nil {
					return
				}
				s.writeResponse(conn, hdr.CorrelationID, resp)
				continue
			case protocol.APKSaslAuthenticate:
				resp, ok, failed, err := s.handleSASLAuthenticate(st, hdr, body)
				if err != nil {
					return
				}
				st.authed = ok
				s.writeResponse(conn, hdr.CorrelationID, resp)
				if failed {
					st.saslFailures++
					s.recordAuthFailure(conn, st)
					if st.saslFailures >= s.maxSASLFailures {
						log.Printf("closing connection from %s after %d failed SASL attempts", remoteAddr(conn), st.saslFailures)
						return
					}
				}
				continue
			default:
				// Unauthenticated non-SASL request: reject the connection.
				log.Printf("rejecting unauthenticated request key=%d", hdr.ApiKey)
				audit.Log(audit.Event{
					Actor:      audit.Anonymous,
					Action:     "kafka.request",
					Resource:   fmt.Sprintf("apikey:%d", hdr.ApiKey),
					Result:     audit.ResultDeny,
					Detail:     "unauthenticated request before SASL",
					RemoteAddr: remoteAddr(conn),
				})
				return
			}
		}

		var localPort int32
		if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok {
			localPort = int32(tcp.Port)
		}
		clientAddr := ""
		if ra := conn.RemoteAddr(); ra != nil {
			clientAddr = ra.String()
		}
		respBody, err := s.handler.Handle(hdr.ApiKey, hdr.ApiVersion, body, handler.RequestContext{
			Principal:    st.principal,
			ListenerPort: localPort,
			ClientID:     hdr.ClientID,
			ClientAddr:   clientAddr,
		})
		if err != nil {
			log.Printf("request error key=%d v=%d: %v", hdr.ApiKey, hdr.ApiVersion, err)
			return
		}
		s.writeResponse(conn, hdr.CorrelationID, respBody)
	}
}

// supportedMechanisms returns the SASL mechanisms this broker implements.
// PLAIN is withheld from an unencrypted connection when security.require_tls_for_plain
// is set, which keeps a *compliant* client from sending its password: it sees
// SCRAM in the list and uses it. A client that skips the handshake has already
// put the password on the wire, so the authenticate step refuses to validate it
// (see handleSASLAuthenticate) — the steering prevents the exposure, it cannot
// undo one.
func (s *Server) supportedMechanisms(encrypted bool) []string {
	if !s.cfg.Security.Enabled {
		return nil
	}
	if s.cfg.Security.RequireTLSForPlain && !encrypted {
		return []string{"SCRAM-SHA-256", "SCRAM-SHA-512"}
	}
	return []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
}

// handleSASLHandshake responds with the supported SASL mechanisms and records
// the mechanism the client selected.
func (s *Server) handleSASLHandshake(st *connState, hdr *protocol.RequestHeader, body []byte) ([]byte, error) {
	req, err := protocol.DecodeSASLHandshakeRequest(hdr.ApiVersion, body)
	if err != nil {
		return nil, err
	}
	offered := s.supportedMechanisms(st.tls)
	resp := &protocol.SASLHandshakeResponse{
		Version:    hdr.ApiVersion,
		ErrorCode:  protocol.ErrNone,
		Mechanisms: offered,
	}
	allowed := false
	for _, m := range offered {
		if m == req.Mechanism {
			allowed = true
			break
		}
	}
	if allowed {
		st.mechanism = req.Mechanism
	} else {
		// Covers an unknown mechanism and PLAIN withheld for being unencrypted:
		// the client learns to retry with one of the advertised mechanisms.
		resp.ErrorCode = protocol.ErrIllegalSaslState
	}
	return protocol.EncodeSASLHandshakeResponse(resp)
}

// handleSASLAuthenticate validates one SASL exchange step. For PLAIN a single
// token authenticates; for SCRAM the exchange spans multiple SaslAuthenticate
// calls (client-first -> server-first -> client-final -> server-final). It
// returns ok=true once the connection is authenticated and failed=true when the
// step was a rejection (as opposed to a challenge awaiting the next step).
func (s *Server) handleSASLAuthenticate(st *connState, hdr *protocol.RequestHeader, body []byte) (resp []byte, ok bool, failed bool, err error) {
	req, err := protocol.DecodeSaslAuthenticateRequest(hdr.ApiVersion, body)
	if err != nil {
		return nil, false, false, err
	}
	out := &protocol.SaslAuthenticateResponse{Version: hdr.ApiVersion, SessionLifetimeMs: 0}

	switch st.mechanism {
	case "PLAIN", "":
		if st.mechanism == "" {
			// No SaslHandshake was performed, so the client sent a PLAIN token
			// without the broker agreeing to PLAIN. The password is already in
			// the frame we read, and this broker will not validate it.
			return s.saslReject(out, "SASL handshake required before authentication")
		}
		// SASL/PLAIN hands the password to the broker, so it is accepted only
		// on an encrypted connection (or when the operator turned the check
		// off for a trusted link).
		if s.cfg.Security.RequireTLSForPlain && !st.tls {
			return s.saslReject(out, "SASL/PLAIN requires TLS: use SCRAM or connect over the TLS listener")
		}
		if user, ok := s.authenticatePlain(req.AuthBytes); ok {
			st.principal = user
			out.ErrorCode = protocol.ErrNone
			encoded, err := protocol.EncodeSaslAuthenticateResponse(out)
			return encoded, true, false, err
		}
		return s.saslReject(out, "SASL authentication failed")

	case "SCRAM-SHA-256", "SCRAM-SHA-512":
		return s.handleSCRAM(st, hdr.ApiVersion, req.AuthBytes)

	default:
		return s.saslReject(out, "no SASL mechanism selected")
	}
}

// saslReject builds a uniform rejection response. The message never says
// whether the user exists: "unknown user" and "wrong password" are the same
// answer, so SCRAM cannot be used to enumerate accounts.
func (s *Server) saslReject(resp *protocol.SaslAuthenticateResponse, reason string) ([]byte, bool, bool, error) {
	resp.ErrorCode = protocol.ErrSaslAuthenticationFailed
	resp.ErrorMessage = &reason
	out, err := protocol.EncodeSaslAuthenticateResponse(resp)
	return out, false, true, err
}

// handleSCRAM drives the 4-step SCRAM exchange using the SaslAuthenticate
// payload as the message carrier.
func (s *Server) handleSCRAM(st *connState, version int16, authBytes []byte) ([]byte, bool, bool, error) {
	resp := &protocol.SaslAuthenticateResponse{Version: version, SessionLifetimeMs: 0}
	msg := string(authBytes)

	// Step 1: client-first-message arrives with no prior session.
	if st.scram == nil {
		mech, ok := credential.MechanismByName(st.mechanism)
		if !ok {
			return s.saslReject(resp, "unsupported SASL mechanism")
		}
		username := scramUsername(msg)
		// Every client-first is answered with a server-first of the same shape:
		// a user that does not exist gets a decoy verifier derived from the
		// same per-user salt, so the exchange costs the same and looks the same,
		// and the failure surfaces only at the proof step.
		verifier, err := s.scramVerifierFor(mech, username)
		if err != nil {
			// The reason (a credential stored as a password hash, a verifier for
			// the other hash family) is logged locally and never sent: the reply
			// must not reveal whether the user exists or how it is stored.
			log.Printf("sasl: no usable SCRAM credential for %q: %v", username, err)
			return s.saslReject(resp, "authentication failed")
		}
		session, serverFirst, err := newScramSession(mech, verifier, msg)
		if err != nil {
			log.Printf("sasl: scram client-first rejected: %v", err)
			return s.saslReject(resp, "authentication failed")
		}
		st.scram = session
		st.scramUsername = username
		resp.ErrorCode = protocol.ErrNone
		resp.AuthBytes = []byte(serverFirst)
		out, err := protocol.EncodeSaslAuthenticateResponse(resp)
		return out, false, false, err
	}

	// Step 2: client-final-message.
	final, err := st.scram.finish(msg)
	if err != nil {
		// One answer for every failure mode: a nonce mismatch, a bad proof, and
		// an unknown user are indistinguishable to the caller.
		return s.saslReject(resp, "authentication failed")
	}
	st.principal = st.scramUsername
	resp.ErrorCode = protocol.ErrNone
	resp.AuthBytes = final
	out, err := protocol.EncodeSaslAuthenticateResponse(resp)
	return out, true, false, err
}

// scramVerifierFor returns the SCRAM verifier for a username. A user that does
// not exist gets a decoy verifier: the server does the same PBKDF2 work and
// sends the same message, so timing and response shape cannot be used to
// enumerate accounts.
func (s *Server) scramVerifierFor(mech credential.Mechanism, username string) (credential.Verifier, error) {
	salt := s.scramSalt(mech, username)
	iterations := s.scramIterations()
	user := s.findUser(username)
	switch {
	case user == nil:
		return credential.DeriveVerifier(mech, "pocketkafka-decoy-"+username, salt, iterations)
	case user.SCRAMVerifier != "":
		v, err := credential.ParseVerifier(user.SCRAMVerifier)
		if err != nil {
			return credential.Verifier{}, err
		}
		if v.Mechanism.Name != mech.Name {
			return credential.Verifier{}, fmt.Errorf("stored SCRAM verifier is %s, client asked for %s", v.Mechanism.Name, mech.Name)
		}
		// A stored verifier is parsed, not derived, so it would answer
		// measurably faster than a plaintext-password account or than the decoy
		// sent for an unknown user — a timing oracle for "this account exists and
		// uses a verifier". Spending the same KDF work (discarded) removes it.
		if _, err := credential.DeriveVerifier(mech, "pocketkafka-step1-padding-"+username, salt, v.Iterations); err != nil {
			return credential.Verifier{}, err
		}
		return v, nil
	case user.Password != "":
		return credential.DeriveVerifier(mech, user.Password, salt, iterations)
	default:
		// password_hash-only user: SCRAM cannot be served without the password.
		return credential.Verifier{}, fmt.Errorf("user has no SCRAM credential")
	}
}

// scramSalt returns the per-user SCRAM salt. It is derived from the server
// secret and the username instead of being random per attempt, so a client that
// caches the salt still works after a restart, and the same salt is produced
// for a user that does not exist (see scramVerifierFor).
func (s *Server) scramSalt(mech credential.Mechanism, username string) []byte {
	mac := hmac.New(sha256.New, []byte(s.cfg.Web.AuthSecret))
	mac.Write([]byte("pocketkafka-scram-salt|" + mech.Name + "|" + username))
	return mac.Sum(nil)[:32]
}

// scramIterations returns the configured PBKDF2 iteration count.
func (s *Server) scramIterations() int {
	if n := s.cfg.Security.SCRAMIterations; n > 0 {
		return n
	}
	return credential.DefaultSCRAMIterations
}

func (s *Server) findUser(username string) *config.SecurityUser {
	for i := range s.cfg.Security.Users {
		if s.cfg.Security.Users[i].Username == username {
			return &s.cfg.Security.Users[i]
		}
	}
	return nil
}

// scramUsername extracts the username from a SCRAM client-first message,
// decoding the RFC 5802 "=2C"/"=3D" escapes.
func scramUsername(clientFirst string) string {
	rest := clientFirst
	if idx := strings.Index(clientFirst, ",,"); idx >= 0 {
		rest = clientFirst[idx+2:]
	} else if idx := strings.Index(clientFirst, ",a="); idx >= 0 {
		if next := strings.Index(clientFirst[idx+3:], ","); next >= 0 {
			rest = clientFirst[idx+3+next+1:]
		}
	}
	for _, part := range strings.Split(rest, ",") {
		if strings.HasPrefix(part, "n=") {
			return unescapeSCRAMName(strings.TrimPrefix(part, "n="))
		}
	}
	return ""
}

// authenticatePlain checks a SASL/PLAIN token ("authzid\0authcid\0passwd") and
// returns the authenticated username so it can become the request principal.
// The comparison covers plaintext and PBKDF2-hashed credentials and is
// constant-time in both cases.
func (s *Server) authenticatePlain(token []byte) (string, bool) {
	parts := strings.SplitN(string(token), "\x00", 3)
	if len(parts) != 3 {
		return "", false
	}
	username, password := parts[1], parts[2]
	for i := range s.cfg.Security.Users {
		u := &s.cfg.Security.Users[i]
		if u.Username != username || u.Username == "" {
			continue
		}
		if u.Password == "" && u.PasswordHash == "" {
			return "", false
		}
		stored := u.Password
		if stored == "" {
			stored = u.PasswordHash
		}
		if credential.Matches(stored, password) {
			return username, true
		}
		return "", false
	}
	return "", false
}

// clientCertPrincipal returns the principal named by a verified client
// certificate, or "" when the connection is not mTLS-authenticated.
func (s *Server) clientCertPrincipal(conn net.Conn) string {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return ""
	}
	// The handshake runs lazily; force it now that the listener requires a
	// client certificate, and treat a failed handshake as "no principal".
	if err := tc.Handshake(); err != nil {
		return ""
	}
	state := tc.ConnectionState()
	if len(state.VerifiedChains) == 0 {
		return ""
	}
	return config.ClientPrincipal(&state)
}

// isTLSConn reports whether a connection is encrypted.
func isTLSConn(conn net.Conn) bool {
	_, ok := conn.(*tls.Conn)
	return ok
}

// remoteHost renders a connection's peer *address* without its ephemeral port.
// It is the limiter key: keying by ip:port would give every reconnect its own
// budget, which is exactly what a brute-force client wants.
func remoteHost(conn net.Conn) string {
	if ra := conn.RemoteAddr(); ra != nil {
		if host, _, err := net.SplitHostPort(ra.String()); err == nil {
			return host
		}
		return ra.String()
	}
	return ""
}

// remoteAddr renders a connection's peer address for logs and the audit trail.
func remoteAddr(conn net.Conn) string {
	if ra := conn.RemoteAddr(); ra != nil {
		return ra.String()
	}
	return ""
}

// recordAuthFailure applies the per-source backoff and writes the audit record
// for one rejected SASL attempt.
func (s *Server) recordAuthFailure(conn net.Conn, st *connState) {
	addr := remoteAddr(conn)
	// Fail on the address (no port) so the next connection from the same source
	// meets the backoff installed here.
	delay := s.saslLimiter.Fail(remoteHost(conn))
	detail := fmt.Sprintf("mechanism=%s attempts=%d", st.mechanism, st.saslFailures)
	if delay > 0 {
		detail += fmt.Sprintf(" backoff_ms=%d", delay.Milliseconds())
	}
	audit.Log(audit.Event{
		Actor:      audit.Anonymous,
		Action:     "auth.sasl",
		Resource:   st.mechanism,
		Result:     audit.ResultDeny,
		Detail:     detail,
		RemoteAddr: addr,
	})
}

// writeResponse writes a length-prefixed response frame.
func (s *Server) writeResponse(conn net.Conn, corrID int32, body []byte) {
	// The ApiVersions response header is always classic (non-flexible) per
	// KIP-482, even when the request version is >= 3 (flexible body).
	frame := protocol.WriteResponseFrame(corrID, body)
	if s.writeTimeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
			return
		}
	}
	if _, err := conn.Write(frame); err != nil {
		return
	}
}

// Close shuts down all listeners, tears down live connections, and waits for
// the connection goroutines to finish. Tearing connections down keeps Close
// from blocking on idle clients that never disconnect.
func (s *Server) Close() error {
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	for _, ln := range s.listeners {
		ln.Close()
	}
	s.connMu.Lock()
	s.closing = true
	for conn := range s.conns {
		conn.Close()
	}
	s.connMu.Unlock()
	s.wg.Wait()
	return nil
}
