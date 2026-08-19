package protocol

import "encoding/binary"

// Writer is a BigEndian binary encoder that appends to an in-memory byte slice.
type Writer struct {
	buf []byte
}

// NewWriter returns a Writer with the given initial capacity.
func NewWriter(capacity int) *Writer {
	return &Writer{buf: make([]byte, 0, capacity)}
}

// Bytes returns the encoded bytes.
func (w *Writer) Bytes() []byte { return w.buf }

// Len returns the number of bytes written so far.
func (w *Writer) Len() int { return len(w.buf) }

// WriteInt8 appends a signed 1-byte integer.
func (w *Writer) WriteInt8(v int8) {
	w.buf = append(w.buf, byte(v))
}

// WriteBool appends a boolean as a 1-byte integer.
func (w *Writer) WriteBool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

// WriteInt16 appends a signed 2-byte integer.
func (w *Writer) WriteInt16(v int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	w.buf = append(w.buf, b[:]...)
}

// WriteInt32 appends a signed 4-byte integer.
func (w *Writer) WriteInt32(v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	w.buf = append(w.buf, b[:]...)
}

// WriteInt64 appends a signed 8-byte integer.
func (w *Writer) WriteInt64(v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.buf = append(w.buf, b[:]...)
}

// WriteUVarint appends an unsigned LEB128 varint.
func (w *Writer) WriteUVarint(v uint64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	w.buf = append(w.buf, b[:n]...)
}

// WriteVarint appends a signed zig-zag varint.
func (w *Writer) WriteVarint(v int64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutVarint(b[:], v)
	w.buf = append(w.buf, b[:n]...)
}

// WriteString appends a classic string with an int16 length prefix. An empty
// string is written as length 0.
func (w *Writer) WriteString(s string) {
	w.WriteInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

// WriteNullableString appends a nullable classic string (-1 when nil).
func (w *Writer) WriteNullableString(s *string) {
	if s == nil {
		w.WriteInt16(-1)
		return
	}
	w.WriteString(*s)
}

// WriteCompactString appends a compact string (uvarint length + 1; 0 = null).
func (w *Writer) WriteCompactString(s string) {
	w.WriteUVarint(uint64(len(s) + 1))
	w.buf = append(w.buf, s...)
}

// WriteCompactNullableString appends a nullable compact string (0 = null).
func (w *Writer) WriteCompactNullableString(s *string) {
	if s == nil {
		w.WriteUVarint(0)
		return
	}
	w.WriteCompactString(*s)
}

// WriteBytes appends a classic byte array with an int32 length prefix.
func (w *Writer) WriteBytes(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(b)))
	w.buf = append(w.buf, b...)
}

// WriteCompactBytes appends a compact byte array (uvarint length + 1).
func (w *Writer) WriteCompactBytes(b []byte) {
	if b == nil {
		w.WriteUVarint(0)
		return
	}
	w.WriteUVarint(uint64(len(b) + 1))
	w.buf = append(w.buf, b...)
}

// WriteArrayLen appends a classic array length (int32). Use -1 for null arrays.
func (w *Writer) WriteArrayLen(n int) {
	w.WriteInt32(int32(n))
}

// WriteCompactArrayLen appends a compact array length (uvarint; 0 = null).
func (w *Writer) WriteCompactArrayLen(n int) {
	if n < 0 {
		n = 0
	}
	w.WriteUVarint(uint64(n + 1))
}

// WriteTaggedFields appends an empty tagged fields section for flexible versions.
func (w *Writer) WriteTaggedFields() {
	w.WriteUVarint(0)
}

func appendVarint(dst []byte, v int64) []byte {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutVarint(b[:], v)
	return append(dst, b[:n]...)
}

func appendUvarint(dst []byte, v uint64) []byte {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	return append(dst, b[:n]...)
}
