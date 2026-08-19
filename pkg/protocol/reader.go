package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Reader is a BigEndian binary decoder over an in-memory byte slice. All Kafka
// integers are big-endian; variable-length integers are LEB128.
type Reader struct {
	buf []byte
	off int
}

// NewReader returns a Reader positioned at the start of buf.
func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf, off: 0}
}

// Remaining returns the number of unread bytes.
func (r *Reader) Remaining() int { return len(r.buf) - r.off }

// Err returns whether the reader has consumed exactly all bytes (nil if so).
func (r *Reader) Err() error {
	if r.off != len(r.buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (r *Reader) need(n int) error {
	if r.off+n > len(r.buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// ReadInt8 reads a signed 1-byte integer.
func (r *Reader) ReadInt8() (int8, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := int8(r.buf[r.off])
	r.off++
	return v, nil
}

// ReadBool reads a 1-byte boolean.
func (r *Reader) ReadBool() (bool, error) {
	v, err := r.ReadInt8()
	return v != 0, err
}

// ReadInt16 reads a signed 2-byte integer.
func (r *Reader) ReadInt16() (int16, error) {
	if err := r.need(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(r.buf[r.off:]))
	r.off += 2
	return v, nil
}

// ReadInt32 reads a signed 4-byte integer.
func (r *Reader) ReadInt32() (int32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.off:]))
	r.off += 4
	return v, nil
}

// ReadInt64 reads a signed 8-byte integer.
func (r *Reader) ReadInt64() (int64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.off:]))
	r.off += 8
	return v, nil
}

// ReadUVarint reads an unsigned LEB128 varint.
func (r *Reader) ReadUVarint() (uint64, error) {
	v, n := binary.Uvarint(r.buf[r.off:])
	if n <= 0 {
		return 0, errors.New("invalid uvarint")
	}
	r.off += n
	return v, nil
}

// ReadVarint reads a signed zig-zag varint.
func (r *Reader) ReadVarint() (int64, error) {
	v, n := binary.Varint(r.buf[r.off:])
	if n <= 0 {
		return 0, errors.New("invalid varint")
	}
	r.off += n
	return v, nil
}

// ReadString reads a classic string: int16 length prefix, -1 means null.
func (r *Reader) ReadString() (string, error) {
	s, _, err := r.readString(true)
	return s, err
}

// ReadNullableString reads a classic string and returns nil if the length was -1.
func (r *Reader) ReadNullableString() (*string, error) {
	s, null, err := r.readString(true)
	if err != nil || null {
		return nil, err
	}
	return &s, nil
}

func (r *Reader) readString(nullable bool) (string, bool, error) {
	l, err := r.ReadInt16()
	if err != nil {
		return "", false, err
	}
	if l < 0 {
		return "", true, nil
	}
	if err := r.need(int(l)); err != nil {
		return "", false, err
	}
	s := string(r.buf[r.off : r.off+int(l)])
	r.off += int(l)
	return s, false, nil
}

// ReadCompactString reads a compact string (uvarint length + 1, 0 = null).
func (r *Reader) ReadCompactString() (string, error) {
	l, err := r.ReadUVarint()
	if err != nil {
		return "", err
	}
	if l == 0 {
		return "", nil
	}
	n := int(l - 1)
	if err := r.need(n); err != nil {
		return "", err
	}
	s := string(r.buf[r.off : r.off+n])
	r.off += n
	return s, nil
}

// ReadBytes reads a classic byte array (int32 length prefix, -1 = null).
func (r *Reader) ReadBytes() ([]byte, error) {
	l, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}
	if err := r.need(int(l)); err != nil {
		return nil, err
	}
	out := make([]byte, l)
	copy(out, r.buf[r.off:r.off+int(l)])
	r.off += int(l)
	return out, nil
}

// ReadCompactBytes reads a compact byte array (uvarint length + 1, 0 = null).
func (r *Reader) ReadCompactBytes() ([]byte, error) {
	l, err := r.ReadUVarint()
	if err != nil {
		return nil, err
	}
	if l == 0 {
		return nil, nil
	}
	n := int(l - 1)
	if err := r.need(n); err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out, r.buf[r.off:r.off+n])
	r.off += n
	return out, nil
}

// ReadArrayLen reads a classic array length (int32, -1 = null).
func (r *Reader) ReadArrayLen() (int, error) {
	l, err := r.ReadInt32()
	if err != nil {
		return 0, err
	}
	if l < 0 {
		return -1, nil
	}
	return int(l), nil
}

// ReadCompactArrayLen reads a compact array length (uvarint, 0 = null).
func (r *Reader) ReadCompactArrayLen() (int, error) {
	l, err := r.ReadUVarint()
	if err != nil {
		return 0, err
	}
	if l == 0 {
		return 0, nil
	}
	return int(l - 1), nil
}

// ReadTaggedFields skips the tagged field section for flexible versions.
func (r *Reader) ReadTaggedFields() error {
	n, err := r.ReadUVarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < n; i++ {
		if _, err := r.ReadUVarint(); err != nil { // tag id
			return err
		}
		size, err := r.ReadUVarint()
		if err != nil {
			return err
		}
		if err := r.need(int(size)); err != nil {
			return err
		}
		r.off += int(size)
	}
	return nil
}
