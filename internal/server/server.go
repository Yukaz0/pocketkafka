// Package server implements the TCP network listener and request dispatcher for
// the broker. It reads length-prefixed Kafka frames and routes them to the
// handler package.
package server

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// Server accepts TCP connections and dispatches Kafka requests.
type Server struct {
	cfg        *config.Config
	handler    *handler.Handler
	bindPorts  map[net.Listener]int32 // port bind per listener (untuk advertise per-listener)
	maxReqSize int64
	listeners  []net.Listener
	tlsConf    *tls.Config
	wg         sync.WaitGroup
	closeCh    chan struct{}
}

// connState carries per-connection authentication state.
type connState struct {
	authed        bool
	mechanism     string // "PLAIN" or "SCRAM-SHA-256"/"SCRAM-SHA-512"
	scram         *scramSession
	scramUsername string
}

// New creates a Server that routes requests to h.
func New(cfg *config.Config, h *handler.Handler) *Server {
	return &Server{
		cfg:        cfg,
		handler:    h,
		bindPorts:  make(map[net.Listener]int32),
		maxReqSize: cfg.Network.MaxRequestSizeBytes,
		closeCh:    make(chan struct{}),
	}
}

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

	// Optional TLS (SSL) listener.
	if s.cfg.Security.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(s.cfg.Security.TLS.CertFile, s.cfg.Security.TLS.KeyFile)
		if err != nil {
			s.Close()
			return fmt.Errorf("load TLS cert: %w", err)
		}
		s.tlsConf = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		ln, err := net.Listen("tcp", s.cfg.Security.TLS.Listen)
		if err != nil {
			s.Close()
			return fmt.Errorf("listen TLS on %s: %w", s.cfg.Security.TLS.Listen, err)
		}
		s.listeners = append(s.listeners, ln)
		log.Printf("pocketkafka TLS listening on %s", s.cfg.Security.TLS.Listen)
	}

	// Register advertised address per bind port: koneksi yang masuk lewat
	// listener N dijawab metadata dengan advertised listener pasangannya,
	// bukan satu alamat global (pencegah reconnect-storm klien docker yang
	// diberi localhost padahal jalurnya jaringan internal).
	for name, addr := range s.cfg.Listeners {
		bindHost, bindPort, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		adv, ok := s.cfg.AdvertisedListeners[name]
		if !ok || adv == "" {
			adv = advertisedPrimary(s.cfg)
		}
		advHost, advPortStr, err := net.SplitHostPort(adv)
		if err != nil {
			continue
		}
		var advPort int64
		fmt.Sscanf(strings.TrimSpace(advPortStr), "%d", &advPort)
		_ = bindHost
		s.handler.SetAdvertisedForPort(int32(advPort), advHost, int32(advPort))
		_ = bindPort
	}

	for _, ln := range s.listeners {
		s.wg.Add(1)
		go s.acceptLoop(ln)
	}
	return nil
}

// advertisedPrimary returns the primary advertised address (same rule as the
// cmd/server helpers, duplicated here to keep the server package decoupled).
func advertisedPrimary(cfg *config.Config) string {
	if a, ok := cfg.AdvertisedListeners["plain"]; ok && a != "" {
		return a
	}
	for _, v := range cfg.AdvertisedListeners {
		return v
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
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	tcp, ok := conn.(*net.TCPConn)
	if ok {
		tcp.SetKeepAlive(true)
		tcp.SetNoDelay(true)
	}

	// When SASL is enabled, a connection must authenticate before issuing any
	// non-SASL request.
	st := &connState{authed: !s.cfg.Security.Enabled}

	for {
		hdr, body, err := protocol.ReadRequestFrame(conn, s.maxReqSize)
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
				resp, ok, err := s.handleSASLAuthenticate(st, hdr, body)
				if err != nil {
					return
				}
				st.authed = ok
				s.writeResponse(conn, hdr.CorrelationID, resp)
				continue
			default:
				// Unauthenticated non-SASL request: reject the connection.
				log.Printf("rejecting unauthenticated request key=%d", hdr.ApiKey)
				return
			}
		}

		var localPort int32
		if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok {
			localPort = int32(tcp.Port)
		}
		respBody, err := s.handler.Handle(hdr.ApiKey, hdr.ApiVersion, body, localPort)
		if err != nil {
			log.Printf("request error key=%d v=%d: %v", hdr.ApiKey, hdr.ApiVersion, err)
			return
		}
		s.writeResponse(conn, hdr.CorrelationID, respBody)
	}
}

// supportedMechanisms returns the SASL mechanisms this broker implements.
func (s *Server) supportedMechanisms() []string {
	if !s.cfg.Security.Enabled {
		return nil
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
	resp := &protocol.SASLHandshakeResponse{
		Version:    hdr.ApiVersion,
		ErrorCode:  protocol.ErrNone,
		Mechanisms: s.supportedMechanisms(),
	}
	switch req.Mechanism {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		st.mechanism = req.Mechanism
	default:
		resp.ErrorCode = protocol.ErrIllegalSaslState
	}
	return protocol.EncodeSASLHandshakeResponse(resp)
}

// handleSASLAuthenticate validates one SASL exchange step. For PLAIN a single
// token authenticates; for SCRAM the exchange spans multiple SaslAuthenticate
// calls (client-first -> server-first -> client-final -> server-final).
func (s *Server) handleSASLAuthenticate(st *connState, hdr *protocol.RequestHeader, body []byte) ([]byte, bool, error) {
	req, err := protocol.DecodeSaslAuthenticateRequest(hdr.ApiVersion, body)
	if err != nil {
		return nil, false, err
	}
	resp := &protocol.SaslAuthenticateResponse{Version: hdr.ApiVersion, SessionLifetimeMs: 0}

	switch st.mechanism {
	case "PLAIN", "":
		if s.authenticatePlain(req.AuthBytes) {
			resp.ErrorCode = protocol.ErrNone
			out, err := protocol.EncodeSaslAuthenticateResponse(resp)
			return out, true, err
		}
		resp.ErrorCode = protocol.ErrSaslAuthenticationFailed
		msg := "SASL authentication failed"
		resp.ErrorMessage = &msg
		out, _ := protocol.EncodeSaslAuthenticateResponse(resp)
		return out, false, nil

	case "SCRAM-SHA-256", "SCRAM-SHA-512":
		return s.handleSCRAM(st, hdr.ApiVersion, req.AuthBytes)
	default:
		resp.ErrorCode = protocol.ErrIllegalSaslState
		msg := "no SASL mechanism selected"
		resp.ErrorMessage = &msg
		out, _ := protocol.EncodeSaslAuthenticateResponse(resp)
		return out, false, nil
	}
}

// handleSCRAM drives the 4-step SCRAM exchange using the SaslAuthenticate
// payload as the message carrier.
func (s *Server) handleSCRAM(st *connState, version int16, authBytes []byte) ([]byte, bool, error) {
	resp := &protocol.SaslAuthenticateResponse{Version: version, SessionLifetimeMs: 0}
	msg := string(authBytes)

	// Step 1: client-first-message arrives with no prior session.
	if st.scram == nil {
		mech := scramSHA256
		if st.mechanism == "SCRAM-SHA-512" {
			mech = scramSHA512
		}
		username := scramUsername(msg)
		user := s.findUser(username)
		if user == nil {
			return s.scramErrorResponse(resp, "unknown user")
		}
		cred := newScramCredential(mech, user.Password, randomSalt(mech.keySize), mech.iterations)
		session, serverFirst, err := newScramSession(mech, cred, msg)
		if err != nil {
			return s.scramErrorResponse(resp, err.Error())
		}
		st.scram = session
		st.scramUsername = username
		resp.ErrorCode = protocol.ErrNone
		resp.AuthBytes = []byte(serverFirst)
		out, _ := protocol.EncodeSaslAuthenticateResponse(resp)
		return out, false, nil
	}

	// Step 2: client-final-message.
	final, err := st.scram.finish(msg)
	if err != nil {
		return s.scramErrorResponse(resp, err.Error())
	}
	resp.ErrorCode = protocol.ErrNone
	resp.AuthBytes = final
	out, _ := protocol.EncodeSaslAuthenticateResponse(resp)
	return out, true, nil
}

func (s *Server) scramErrorResponse(resp *protocol.SaslAuthenticateResponse, reason string) ([]byte, bool, error) {
	resp.ErrorCode = protocol.ErrSaslAuthenticationFailed
	resp.ErrorMessage = &reason
	out, _ := protocol.EncodeSaslAuthenticateResponse(resp)
	return out, false, nil
}

func (s *Server) findUser(username string) *config.SecurityUser {
	for i := range s.cfg.Security.Users {
		if s.cfg.Security.Users[i].Username == username {
			return &s.cfg.Security.Users[i]
		}
	}
	return nil
}

// scramUsername extracts the username from a SCRAM client-first message.
func scramUsername(clientFirst string) string {
	// gs2 header "n,," followed by "n=<user>,r=<nonce>[,extensions]"
	rest := clientFirst
	if idx := strings.Index(clientFirst, ",,"); idx >= 0 {
		rest = clientFirst[idx+2:]
	}
	for _, part := range strings.Split(rest, ",") {
		if strings.HasPrefix(part, "n=") {
			return strings.TrimPrefix(part, "n=")
		}
	}
	return ""
}

// randomSalt returns a cryptographically random SCRAM salt.
func randomSalt(n int) []byte {
	salt := make([]byte, n)
	if _, err := rand.Read(salt); err != nil {
		return []byte("pocketkafka-salt")
	}
	return salt
}

// authenticatePlain checks a SASL/PLAIN token ("authzid\0authcid\0passwd").
func (s *Server) authenticatePlain(token []byte) bool {
	parts := strings.SplitN(string(token), "\x00", 3)
	if len(parts) != 3 {
		return false
	}
	username, password := parts[1], parts[2]
	for _, u := range s.cfg.Security.Users {
		if u.Username == username && u.Password == password {
			return true
		}
	}
	return false
}

// writeResponse writes a length-prefixed response frame.
func (s *Server) writeResponse(conn net.Conn, corrID int32, body []byte) {
	// The ApiVersions response header is always classic (non-flexible) per
	// KIP-482, even when the request version is >= 3 (flexible body).
	frame := protocol.WriteResponseFrame(corrID, body)
	if _, err := conn.Write(frame); err != nil {
		return
	}
}

// Close shuts down all listeners and waits for active connections to finish.
func (s *Server) Close() error {
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	for _, ln := range s.listeners {
		ln.Close()
	}
	s.wg.Wait()
	return nil
}
