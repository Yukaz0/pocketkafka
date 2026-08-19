// Package server implements the TCP network listener and request dispatcher for
// the broker. It reads length-prefixed Kafka frames and routes them to the
// handler package.
package server

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/neu/go-kafka-neu/internal/config"
	"github.com/neu/go-kafka-neu/internal/handler"
	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// Server accepts TCP connections and dispatches Kafka requests.
type Server struct {
	cfg        *config.Config
	handler    *handler.Handler
	maxReqSize int64
	listeners  []net.Listener
	tlsConf    *tls.Config
	wg         sync.WaitGroup
	closeCh    chan struct{}
}

// New creates a Server that routes requests to h.
func New(cfg *config.Config, h *handler.Handler) *Server {
	return &Server{
		cfg:        cfg,
		handler:    h,
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
		log.Printf("go-kafka-neu listening on %s", addr)
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
		log.Printf("go-kafka-neu TLS listening on %s", s.cfg.Security.TLS.Listen)
	}

	for _, ln := range s.listeners {
		s.wg.Add(1)
		go s.acceptLoop(ln)
	}
	return nil
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
	authed := !s.cfg.Security.Enabled

	for {
		hdr, body, err := protocol.ReadRequestFrame(conn, s.maxReqSize)
		if err != nil {
			return
		}

		if !authed {
			switch hdr.ApiKey {
			case protocol.APKApiVersions:
				// Allowed pre-auth; falls through to the normal handler.
			case protocol.APKSaslHandshake:
				resp, err := s.handleSASLHandshake(hdr, body)
				if err != nil {
					return
				}
				s.writeResponse(conn, hdr.CorrelationID, resp)
				continue
			case protocol.APKSaslAuthenticate:
				resp, ok, err := s.handleSASLAuthenticate(hdr, body)
				if err != nil {
					return
				}
				authed = ok
				s.writeResponse(conn, hdr.CorrelationID, resp)
				continue
			default:
				// Unauthenticated non-SASL request: reject the connection.
				log.Printf("rejecting unauthenticated request key=%d", hdr.ApiKey)
				return
			}
		}

		respBody, err := s.handler.Handle(hdr.ApiKey, hdr.ApiVersion, body)
		if err != nil {
			log.Printf("request error key=%d v=%d: %v", hdr.ApiKey, hdr.ApiVersion, err)
			return
		}
		s.writeResponse(conn, hdr.CorrelationID, respBody)
	}
}

// handleSASLHandshake responds with the supported SASL mechanism.
func (s *Server) handleSASLHandshake(hdr *protocol.RequestHeader, body []byte) ([]byte, error) {
	req, err := protocol.DecodeSASLHandshakeRequest(hdr.ApiVersion, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.SASLHandshakeResponse{Version: hdr.ApiVersion, ErrorCode: protocol.ErrNone, Mechanisms: []string{"PLAIN"}}
	if req.Mechanism != "PLAIN" {
		resp.ErrorCode = protocol.ErrIllegalSaslState
	}
	return protocol.EncodeSASLHandshakeResponse(resp)
}

// handleSASLAuthenticate validates a SASL/PLAIN token and reports success.
func (s *Server) handleSASLAuthenticate(hdr *protocol.RequestHeader, body []byte) ([]byte, bool, error) {
	req, err := protocol.DecodeSaslAuthenticateRequest(hdr.ApiVersion, body)
	if err != nil {
		return nil, false, err
	}
	resp := &protocol.SaslAuthenticateResponse{Version: hdr.ApiVersion, SessionLifetimeMs: 0}
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
