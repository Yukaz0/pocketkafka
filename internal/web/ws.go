// Package web implements the embedded monitoring dashboard (REST + WebSocket
// live tail) that ships inside the pocketkafka binary on port 8080.
package web

import (
	"crypto/sha1" //#nosec G505 -- required by RFC 6455: Sec-WebSocket-Accept is SHA-1 by definition, and it authenticates nothing

	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// wsGUID is the fixed GUID from RFC 6455 used to compute the accept key.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	// wsMaxControlPayload is the RFC 6455 limit for a control frame.
	wsMaxControlPayload = 125
	// wsMaxClientPayload bounds a data frame coming *from* the browser. The
	// dashboard only ever sends pings, so anything larger is not a dashboard:
	// the previous code allocated whatever length the client declared, up to
	// 2^63 bytes.
	wsMaxClientPayload = 64 << 10
	// wsWriteTimeout bounds a write to a client that stopped reading.
	wsWriteTimeout = 15 * time.Second
)

// wsOriginAllowed reports whether a WebSocket handshake may be accepted. A
// browser always sends Origin on a handshake, so a missing one is not from the
// dashboard; a present one must name this exact host, which stops another site
// (including another port on localhost) from opening a socket with the
// operator's cookie.
func (s *Server) wsOriginAllowed(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Host, r.Host)
}

// wsHostAllowed reports whether the Host header may be served over WebSocket.
// Requiring an IP literal or "localhost" by default defeats DNS rebinding: an
// attacker's domain resolves to 127.0.0.1 but its Host is a name. A dashboard
// reached under another name must list it in web.allowed_hosts.
func (s *Server) wsHostAllowed(r *http.Request) bool {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" {
		return false
	}
	for _, allowed := range s.allowedHosts {
		if strings.EqualFold(strings.TrimSpace(allowed), host) {
			return true
		}
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	return net.ParseIP(host) != nil
}

// upgradeWebSocket performs the RFC 6455 opening handshake. It hijacks the
// connection and returns the raw net.Conn for frame I/O.
func (s *Server) upgradeWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if !headerContains(r.Header.Get("Connection"), "upgrade") {
		return nil, fmt.Errorf("missing Connection: Upgrade")
	}
	if r.Header.Get("Upgrade") != "websocket" {
		return nil, fmt.Errorf("missing Upgrade: websocket")
	}
	if !s.wsHostAllowed(r) {
		return nil, fmt.Errorf("websocket refused: host %q is not allowed (add it to web.allowed_hosts)", r.Host)
	}
	if !s.wsOriginAllowed(r) {
		return nil, fmt.Errorf("websocket refused: Origin must match Host")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, fmt.Errorf("missing Sec-WebSocket-Key")
	}

	h := sha1.New() //#nosec G401 -- see the import: the WebSocket handshake digest is fixed by RFC 6455
	h.Write([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("hijack not supported")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// writeWSFrame writes a single unmasked text frame to the client. The write is
// bounded so a client that stops reading cannot pin a goroutine forever.
func writeWSFrame(conn net.Conn, payload []byte) error {
	var hdr [14]byte
	i := 0
	hdr[i] = 0x81 // FIN + text opcode
	i++
	n := len(payload)
	switch {
	case n <= 125:
		hdr[i] = byte(n)
		i++
	case n <= 65535:
		hdr[i] = 126
		i++
		binary.BigEndian.PutUint16(hdr[i:i+2], uint16(n))
		i += 2
	default:
		hdr[i] = 127
		i++
		binary.BigEndian.PutUint64(hdr[i:i+8], uint64(n))
		i += 8
	}
	if err := conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout)); err != nil {
		return err
	}
	if _, err := conn.Write(hdr[:i]); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

// readWSPing reads one frame from the client: it answers a ping with a pong and
// rejects anything a dashboard client should not send. A rejected frame is a
// fatal error, so the caller closes the connection instead of reading further.
func readWSPing(conn net.Conn) error {
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return err
	}
	fin := hdr[0]&0x80 != 0
	opcode := hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	switch length {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return err
		}
		length = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return err
		}
		length = binary.BigEndian.Uint64(b[:])
	}

	// RFC 6455: a client frame must be masked, a control frame must not be
	// fragmented and must fit in 125 bytes.
	if !masked {
		return errors.New("websocket: unmasked client frame")
	}
	limit := uint64(wsMaxClientPayload)
	if opcode >= 0x8 {
		limit = wsMaxControlPayload
		if !fin {
			return errors.New("websocket: fragmented control frame")
		}
	}
	if length > limit {
		return fmt.Errorf("websocket: client frame of %d bytes exceeds the %d byte limit", length, limit)
	}

	var maskKey [4]byte
	if _, err := io.ReadFull(conn, maskKey[:]); err != nil {
		return err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	for i := range buf {
		buf[i] ^= maskKey[i%4]
	}

	switch opcode {
	case 0x8: // close: end the stream cleanly
		return io.EOF
	case 0x9: // ping: answer with a pong of the same payload
		pong := make([]byte, 0, len(buf)+2)
		pong = append(pong, 0x8A, byte(len(buf)))
		pong = append(pong, buf...)
		if err := conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout)); err != nil {
			return err
		}
		_, err := conn.Write(pong)
		return err
	case 0xA: // pong: nothing to do
		return nil
	default:
		// Text/binary frames are not part of the tail protocol; accepting them
		// only creates a channel nobody reads.
		return fmt.Errorf("websocket: unexpected opcode 0x%x", opcode)
	}
}

// wsDrain reads any frame the client has already sent without waiting for one.
// A read deadline in the past makes the read return immediately; only a timeout
// (nothing pending) is not an error.
func wsDrain(conn net.Conn) error {
	if err := conn.SetReadDeadline(time.Now()); err != nil {
		return err
	}
	err := readWSPing(conn)
	if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		return nil
	}
	return err
}

func headerContains(v, want string) bool {
	// Basic substring check on the comma-separated header value (case-insensitive).
	start := 0
	for i := 0; i <= len(v); i++ {
		if i == len(v) || v[i] == ',' {
			tok := trimSpace(v[start:i])
			if strings.EqualFold(tok, want) {
				return true
			}
			start = i + 1
		}
	}
	return false
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
