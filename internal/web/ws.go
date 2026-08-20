// Package web implements the embedded monitoring dashboard (REST + WebSocket
// live tail) that ships inside the pocketkafka binary on port 8080.
package web

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

// wsGUID is the fixed GUID from RFC 6455 used to compute the accept key.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// upgradeWebSocket performs the RFC 6455 opening handshake. It hijacks the
// connection and returns the raw net.Conn for frame I/O.
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if !headerContains(r.Header.Get("Connection"), "upgrade") {
		return nil, fmt.Errorf("missing Connection: Upgrade")
	}
	if r.Header.Get("Upgrade") != "websocket" {
		return nil, fmt.Errorf("missing Upgrade: websocket")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, fmt.Errorf("missing Sec-WebSocket-Key")
	}

	h := sha1.New()
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

// writeWSFrame writes a single unmasked text frame to the client.
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
	if _, err := conn.Write(hdr[:i]); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

// readWSPing reads one frame from the client, handling close and ping frames
// (responding to pings). It returns nil on a clean close.
func readWSPing(conn net.Conn) error {
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return err
	}
	opcode := hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	if length == 126 {
		var b [2]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return err
		}
		length = uint64(binary.BigEndian.Uint16(b[:]))
	} else if length == 127 {
		var b [8]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return err
		}
		length = binary.BigEndian.Uint64(b[:])
	}
	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(conn, maskKey[:]); err != nil {
			return err
		}
	}
	if opcode == 0x9 { // ping
		buf := make([]byte, length)
		io.ReadFull(conn, buf)
		if masked {
			for i := range buf {
				buf[i] ^= maskKey[i%4]
			}
		}
		// Respond with a pong frame (opcode 0xA).
		var ph [2]byte
		ph[0] = 0x8A
		ph[1] = byte(len(buf))
		conn.Write(ph[:])
		if len(buf) > 0 {
			conn.Write(buf)
		}
		return nil
	}
	// Consume payload of non-ping frames.
	if length > 0 {
		buf := make([]byte, length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return err
		}
	}
	return nil
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
