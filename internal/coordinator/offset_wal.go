package coordinator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	offsetWALVersion = 1

	walRecordCommit byte = 1

	maxWALRecordSize = 1 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type walRecord struct {
	Type        byte
	Group       string
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	Timestamp   int64
}

type offsetWAL struct {
	path string
	f    *os.File
}

func openWAL(path string) (*offsetWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("offset WAL open: %w", err)
	}
	return &offsetWAL{path: path, f: f}, nil
}

func (w *offsetWAL) appendBatch(recs []walRecord) error {
	if len(recs) == 0 {
		return nil
	}
	var buf []byte
	for _, rec := range recs {
		buf = append(buf, encodeWALRecord(rec)...)
	}
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("offset WAL append: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("offset WAL sync: %w", err)
	}
	return nil
}

func (w *offsetWAL) replay(fn func(walRecord)) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := io.NewSectionReader(w.f, 0, 1<<62)
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		n := int64(binary.BigEndian.Uint32(lenBuf[:]))
		if n <= 0 || n > maxWALRecordSize {
			return fmt.Errorf("offset WAL corrupt: bad record length %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		want := binary.BigEndian.Uint32(crcBuf[:])
		if got := crc32.Checksum(payload, castagnoli); got != want {
			return fmt.Errorf("offset WAL corrupt: checksum mismatch (got %08x want %08x)", got, want)
		}
		rec, err := decodeWALPayload(payload)
		if err != nil {
			return err
		}
		fn(rec)
	}
}

func (w *offsetWAL) reset() error {
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("offset WAL truncate: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *offsetWAL) close() error {
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

func encodeWALRecord(rec walRecord) []byte {
	var p []byte
	p = append(p, offsetWALVersion, rec.Type)
	p = appendWALString(p, rec.Group)
	p = appendWALString(p, rec.Topic)
	p = binary.BigEndian.AppendUint32(p, uint32(rec.Partition))
	p = binary.BigEndian.AppendUint64(p, uint64(rec.Offset))
	p = binary.BigEndian.AppendUint32(p, uint32(rec.LeaderEpoch))
	p = appendWALNullableString(p, rec.Metadata)
	p = binary.BigEndian.AppendUint64(p, uint64(rec.Timestamp))

	out := make([]byte, 0, len(p)+8)
	out = binary.BigEndian.AppendUint32(out, uint32(len(p)))
	out = append(out, p...)
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(p, castagnoli))
	return out
}

func decodeWALPayload(p []byte) (walRecord, error) {
	var rec walRecord
	if len(p) < 2 {
		return rec, fmt.Errorf("offset WAL corrupt: short payload")
	}
	if p[0] != offsetWALVersion {
		return rec, fmt.Errorf("offset WAL corrupt: unsupported version %d", p[0])
	}
	rec.Type = p[1]
	pos := 2
	var err error
	if rec.Group, pos, err = readWALString(p, pos); err != nil {
		return rec, err
	}
	if rec.Topic, pos, err = readWALString(p, pos); err != nil {
		return rec, err
	}
	if pos+4 > len(p) {
		return rec, fmt.Errorf("offset WAL corrupt: truncated partition")
	}
	rec.Partition = int32(binary.BigEndian.Uint32(p[pos:]))
	pos += 4
	if pos+8 > len(p) {
		return rec, fmt.Errorf("offset WAL corrupt: truncated offset")
	}
	rec.Offset = int64(binary.BigEndian.Uint64(p[pos:]))
	pos += 8
	if pos+4 > len(p) {
		return rec, fmt.Errorf("offset WAL corrupt: truncated leader epoch")
	}
	rec.LeaderEpoch = int32(binary.BigEndian.Uint32(p[pos:]))
	pos += 4
	if rec.Metadata, pos, err = readWALNullableString(p, pos); err != nil {
		return rec, err
	}
	if pos+8 > len(p) {
		return rec, fmt.Errorf("offset WAL corrupt: truncated timestamp")
	}
	rec.Timestamp = int64(binary.BigEndian.Uint64(p[pos:]))
	return rec, nil
}

func appendWALString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func appendWALNullableString(dst []byte, s *string) []byte {
	if s == nil {
		return binary.BigEndian.AppendUint16(dst, 0xffff)
	}
	return appendWALString(dst, *s)
}

func readWALString(p []byte, pos int) (string, int, error) {
	if pos+2 > len(p) {
		return "", pos, fmt.Errorf("offset WAL corrupt: truncated string length")
	}
	n := int(binary.BigEndian.Uint16(p[pos:]))
	pos += 2
	if pos+n > len(p) {
		return "", pos, fmt.Errorf("offset WAL corrupt: truncated string")
	}
	return string(p[pos : pos+n]), pos + n, nil
}

func readWALNullableString(p []byte, pos int) (*string, int, error) {
	if pos+2 > len(p) {
		return nil, pos, fmt.Errorf("offset WAL corrupt: truncated nullable string length")
	}
	if binary.BigEndian.Uint16(p[pos:]) == 0xffff {
		return nil, pos + 2, nil
	}
	s, next, err := readWALString(p, pos)
	if err != nil {
		return nil, pos, err
	}
	return &s, next, nil
}
