// Package server implements the TCP network listener and request dispatcher for
// the broker. It reads length-prefixed Kafka frames and routes them to the
// handler package.
package server

import (
	"fmt"
	"log"
	"net"
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

	for {
		hdr, body, err := protocol.ReadRequestFrame(conn, s.maxReqSize)
		if err != nil {
			return
		}
		respBody, err := s.handler.Handle(hdr.ApiKey, hdr.ApiVersion, body)
		if err != nil {
			log.Printf("request error key=%d v=%d: %v", hdr.ApiKey, hdr.ApiVersion, err)
			return
		}
		// The ApiVersions response header is always classic (non-flexible) per
		// KIP-482, even when the request version is >= 3 (flexible body).
		frame := protocol.WriteResponseFrame(hdr.CorrelationID, respBody)
		if _, err := conn.Write(frame); err != nil {
			return
		}
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
