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

type Server struct {
	cfg          *config.Config
	handler      *handler.Handler
	bindPorts    map[net.Listener]int32
	maxReqSize   int64
	preAuthSize  int64
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration
	listeners    []net.Listener
	tlsConf      *tls.Config
	wg           sync.WaitGroup
	closeCh      chan struct{}

	saslLimiter     *ratelimit.Limiter
	maxSASLFailures int

	connSem    chan struct{}
	rejections atomic.Int64

	connMu  sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

type connState struct {
	authed        bool
	principal     string
	mechanism     string
	scram         *scramSession
	scramUsername string

	tls bool

	saslFailures int
}

const maxSASLFailuresPerConn = 3

func New(cfg *config.Config, h *handler.Handler) *Server {
	maxConns := cfg.Network.MaxConnections
	if maxConns <= 0 {

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

func (s *Server) RejectedConnections() int64 { return s.rejections.Load() }

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

	if tlsPort != 0 {
		adv, ok := s.cfg.AdvertisedListeners["ssl"]
		if !ok || adv == "" {
			primaryHost, _, _ := splitPort(advertisedPrimary(s.cfg))
			if host, _, err := net.SplitHostPort(s.cfg.Security.TLS.Listen); err == nil &&
				host != "" && host != "0.0.0.0" && host != "::" && !config.IsLoopbackHost(host) {

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

func tlsVersionLabel(min string) string {
	if strings.TrimSpace(min) == "1.3" {
		return "TLS1.3"
	}
	return "TLS1.2"
}

func advertisedPrimary(cfg *config.Config) string {
	if a, ok := cfg.AdvertisedListeners["plain"]; ok && a != "" {
		return a
	}

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

			s.rejections.Add(1)
			log.Printf("rejecting connection from %s: max_connections %d reached", conn.RemoteAddr(), cap(s.connSem))
			conn.Close()
			continue
		}
		if !s.registerConn(conn) {

			conn.Close()
			<-s.connSem
			return
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) registerConn(conn net.Conn) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closing {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) releaseConn(conn net.Conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
	conn.Close()
	<-s.connSem
}

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

	st := &connState{authed: !s.cfg.Security.Enabled}

	if principal := s.clientCertPrincipal(conn); principal != "" {
		st.authed = true
		st.principal = principal
		st.mechanism = "mTLS"
	}
	st.tls = isTLSConn(conn)

	for {

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

func (s *Server) supportedMechanisms(encrypted bool) []string {
	if !s.cfg.Security.Enabled {
		return nil
	}
	if s.cfg.Security.RequireTLSForPlain && !encrypted {
		return []string{"SCRAM-SHA-256", "SCRAM-SHA-512"}
	}
	return []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
}

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

		resp.ErrorCode = protocol.ErrIllegalSaslState
	}
	return protocol.EncodeSASLHandshakeResponse(resp)
}

func (s *Server) handleSASLAuthenticate(st *connState, hdr *protocol.RequestHeader, body []byte) (resp []byte, ok bool, failed bool, err error) {
	req, err := protocol.DecodeSaslAuthenticateRequest(hdr.ApiVersion, body)
	if err != nil {
		return nil, false, false, err
	}
	out := &protocol.SaslAuthenticateResponse{Version: hdr.ApiVersion, SessionLifetimeMs: 0}

	switch st.mechanism {
	case "PLAIN", "":
		if st.mechanism == "" {

			return s.saslReject(out, "SASL handshake required before authentication")
		}

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

func (s *Server) saslReject(resp *protocol.SaslAuthenticateResponse, reason string) ([]byte, bool, bool, error) {
	resp.ErrorCode = protocol.ErrSaslAuthenticationFailed
	resp.ErrorMessage = &reason
	out, err := protocol.EncodeSaslAuthenticateResponse(resp)
	return out, false, true, err
}

func (s *Server) handleSCRAM(st *connState, version int16, authBytes []byte) ([]byte, bool, bool, error) {
	resp := &protocol.SaslAuthenticateResponse{Version: version, SessionLifetimeMs: 0}
	msg := string(authBytes)

	if st.scram == nil {
		mech, ok := credential.MechanismByName(st.mechanism)
		if !ok {
			return s.saslReject(resp, "unsupported SASL mechanism")
		}
		username := scramUsername(msg)

		verifier, err := s.scramVerifierFor(mech, username)
		if err != nil {

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

	final, err := st.scram.finish(msg)
	if err != nil {

		return s.saslReject(resp, "authentication failed")
	}
	st.principal = st.scramUsername
	resp.ErrorCode = protocol.ErrNone
	resp.AuthBytes = final
	out, err := protocol.EncodeSaslAuthenticateResponse(resp)
	return out, true, false, err
}

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

		if _, err := credential.DeriveVerifier(mech, "pocketkafka-step1-padding-"+username, salt, v.Iterations); err != nil {
			return credential.Verifier{}, err
		}
		return v, nil
	case user.Password != "":
		return credential.DeriveVerifier(mech, user.Password, salt, iterations)
	default:

		return credential.Verifier{}, fmt.Errorf("user has no SCRAM credential")
	}
}

func (s *Server) scramSalt(mech credential.Mechanism, username string) []byte {
	mac := hmac.New(sha256.New, []byte(s.cfg.Web.AuthSecret))
	mac.Write([]byte("pocketkafka-scram-salt|" + mech.Name + "|" + username))
	return mac.Sum(nil)[:32]
}

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

func (s *Server) clientCertPrincipal(conn net.Conn) string {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return ""
	}

	if err := tc.Handshake(); err != nil {
		return ""
	}
	state := tc.ConnectionState()
	if len(state.VerifiedChains) == 0 {
		return ""
	}
	return config.ClientPrincipal(&state)
}

func isTLSConn(conn net.Conn) bool {
	_, ok := conn.(*tls.Conn)
	return ok
}

func remoteHost(conn net.Conn) string {
	if ra := conn.RemoteAddr(); ra != nil {
		if host, _, err := net.SplitHostPort(ra.String()); err == nil {
			return host
		}
		return ra.String()
	}
	return ""
}

func remoteAddr(conn net.Conn) string {
	if ra := conn.RemoteAddr(); ra != nil {
		return ra.String()
	}
	return ""
}

func (s *Server) recordAuthFailure(conn net.Conn, st *connState) {
	addr := remoteAddr(conn)

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

func (s *Server) writeResponse(conn net.Conn, corrID int32, body []byte) {

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
