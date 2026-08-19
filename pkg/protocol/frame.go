package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// RequestHeader is the fixed portion of a Kafka request frame.
type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
}

// DecodeRequestHeader parses the header from a Reader positioned just after the
// 4-byte frame length. It assumes a classic (non-flexible) request header.
func DecodeRequestHeader(r *Reader) (*RequestHeader, error) {
	apiKey, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	apiVersion, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	corrID, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	clientID, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationID: corrID,
		ClientID:      clientID,
	}, nil
}

// ReadRequestFrame reads a single length-prefixed request frame from conn. It
// returns the parsed header and the raw request body bytes (everything after
// the header). Requests larger than maxSize are rejected.
func ReadRequestFrame(conn io.Reader, maxSize int64) (*RequestHeader, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, nil, err
	}
	length := int64(binary.BigEndian.Uint32(lenBuf[:]))
	if length <= 0 {
		return nil, nil, fmt.Errorf("invalid frame length %d", length)
	}
	if maxSize > 0 && length > maxSize {
		return nil, nil, fmt.Errorf("frame length %d exceeds max %d", length, maxSize)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, nil, err
	}
	r := NewReader(buf)
	hdr, err := DecodeRequestHeader(r)
	if err != nil {
		return nil, nil, err
	}
	body := buf[r.off:]
	return hdr, body, nil
}

// WriteResponseFrame builds a complete length-prefixed response frame for a
// non-flexible response. body must already contain the correlation ID.
func WriteResponseFrame(corrID int32, body []byte) []byte {
	out := make([]byte, 0, len(body)+8)
	out = appendInt32(out, int32(len(body)+4))
	out = appendInt32(out, corrID)
	out = append(out, body...)
	return out
}

// WriteResponseFrameFlexible builds a response frame whose header includes the
// flexible (KIP-482) tagged fields section. A single zero byte is appended
// after the correlation ID to represent an empty tagged field set.
func WriteResponseFrameFlexible(corrID int32, body []byte) []byte {
	out := make([]byte, 0, len(body)+9)
	out = appendInt32(out, int32(len(body)+5))
	out = appendInt32(out, corrID)
	out = append(out, 0x00) // empty tagged fields
	out = append(out, body...)
	return out
}

func appendInt32(dst []byte, v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return append(dst, b[:]...)
}

// ResponseHeader is the fixed portion of a Kafka response frame.
type ResponseHeader struct {
	CorrelationID int32
}

// EncodeResponseHeader appends a classic (non-flexible) response header.
func EncodeResponseHeader(w *Writer, corrID int32) {
	w.WriteInt32(corrID)
}
