package protocol

import "encoding/binary"

type Writer struct {
	buf []byte
}

func NewWriter(capacity int) *Writer {
	return &Writer{buf: make([]byte, 0, capacity)}
}

func (w *Writer) Bytes() []byte { return w.buf }

func (w *Writer) Len() int { return len(w.buf) }

func (w *Writer) WriteInt8(v int8) {
	w.buf = append(w.buf, byte(v))
}

func (w *Writer) WriteBool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

func (w *Writer) WriteInt16(v int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *Writer) WriteInt32(v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *Writer) WriteInt64(v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *Writer) WriteUVarint(v uint64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	w.buf = append(w.buf, b[:n]...)
}

func (w *Writer) WriteVarint(v int64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutVarint(b[:], v)
	w.buf = append(w.buf, b[:n]...)
}

func (w *Writer) WriteString(s string) {
	w.WriteInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *Writer) WriteNullableString(s *string) {
	if s == nil {
		w.WriteInt16(-1)
		return
	}
	w.WriteString(*s)
}

func (w *Writer) WriteCompactString(s string) {
	w.WriteUVarint(uint64(len(s) + 1))
	w.buf = append(w.buf, s...)
}

func (w *Writer) WriteCompactNullableString(s *string) {
	if s == nil {
		w.WriteUVarint(0)
		return
	}
	w.WriteCompactString(*s)
}

func (w *Writer) WriteBytes(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *Writer) WriteCompactBytes(b []byte) {
	if b == nil {
		w.WriteUVarint(0)
		return
	}
	w.WriteUVarint(uint64(len(b) + 1))
	w.buf = append(w.buf, b...)
}

func (w *Writer) WriteArrayLen(n int) {
	w.WriteInt32(int32(n))
}

func (w *Writer) WriteCompactArrayLen(n int) {
	if n < 0 {
		n = 0
	}
	w.WriteUVarint(uint64(n + 1))
}

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
