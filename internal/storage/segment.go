package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Segment is a single append-only log file (.log) with its associated sparse
// index (.index). It owns the raw RecordBatch bytes for a contiguous range of
// offsets starting at baseOffset.
type Segment struct {
	baseOffset      int64
	logFile         *os.File
	index           *indexFile
	nextOffset      int64 // Log End Offset within this segment
	size            int64 // current .log size in bytes
	dir             string
	maxSegmentBytes int64
	indexInterval   int64
}

// openSegment opens (or creates) a segment whose base offset is baseOffset and
// whose files live in dir. If an existing segment is found it is resumed, which
// recovers nextOffset and size from disk.
func openSegment(dir string, baseOffset int64, maxSegmentBytes, indexInterval int64) (*Segment, error) {
	name := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, name+".log")
	idxPath := filepath.Join(dir, name+".index")

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment log: %w", err)
	}
	st, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, err
	}

	idx, err := openIndex(idxPath, baseOffset, indexInterval)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open segment index: %w", err)
	}

	seg := &Segment{
		baseOffset:      baseOffset,
		logFile:         logFile,
		index:           idx,
		nextOffset:      baseOffset,
		size:            st.Size(),
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		indexInterval:   indexInterval,
	}
	if err := seg.recover(); err != nil {
		seg.close()
		return nil, err
	}
	return seg, nil
}

// recover recomputes nextOffset from the existing log content so a reopened
// segment has an accurate LEO (crash recovery).
func (s *Segment) recover() error {
	if s.size == 0 {
		return nil
	}
	buf := make([]byte, 12)
	pos := int64(0)
	for pos < s.size {
		if _, err := s.logFile.ReadAt(buf, pos); err != nil {
			return fmt.Errorf("recover read: %w", err)
		}
		length := int32(binary.BigEndian.Uint32(buf[8:12]))
		if length <= 0 || pos+12+int64(length) > s.size {
			break // trailing partial batch; stop here
		}
		// LastOffsetDelta lives at offset 23 within the batch (11 into the
		// 12-byte prefix + length region).
		var delta [4]byte
		if _, err := s.logFile.ReadAt(delta[:], pos+23); err != nil {
			return err
		}
		lastOffsetDelta := int32(binary.BigEndian.Uint32(delta[:]))
		s.nextOffset += int64(lastOffsetDelta) + 1
		pos += 12 + int64(length)
	}
	if pos != s.size {
		// A partial batch remains; truncate it.
		if err := s.logFile.Truncate(pos); err != nil {
			return err
		}
		s.size = pos
	}
	return nil
}

// baseOffsetOf returns the base offset of this segment.
func (s *Segment) baseOffsetOf() int64 { return s.baseOffset }

// append writes a raw RecordBatch to the segment, rewriting its base offset to
// the segment's current nextOffset. It returns the assigned base offset.
func (s *Segment) append(raw []byte) (int64, error) {
	h, err := ParseRecordBatchHeader(raw)
	if err != nil {
		return 0, err
	}
	if s.nextOffset+int64(h.LastOffsetDelta)+1-s.baseOffset > int64(^uint32(0)>>1) {
		return 0, fmt.Errorf("segment offset overflow")
	}

	assigned := s.nextOffset
	// Rewrite the base offset in the copy before persisting.
	var base [8]byte
	binary.BigEndian.PutUint64(base[:], uint64(assigned))
	copy(raw[0:8], base[:])

	n, err := s.logFile.WriteAt(raw, s.size)
	if err != nil {
		return 0, err
	}
	if n != len(raw) {
		return 0, fmt.Errorf("short write to segment: %d/%d", n, len(raw))
	}
	pos := s.size
	s.size += int64(n)
	s.nextOffset += int64(h.LastOffsetDelta) + 1

	if err := s.index.maybeAppend(assigned, pos); err != nil {
		return 0, err
	}
	return assigned, nil
}

// full reports whether the segment has reached its configured size limit and
// should be rolled.
func (s *Segment) full() bool {
	return s.size >= s.maxSegmentBytes
}

// read returns raw RecordBatch bytes starting at or containing offset, up to
// maxBytes. Batches whose last offset is strictly before the requested offset
// are skipped.
func (s *Segment) read(offset int64, maxBytes int32) ([]byte, error) {
	pos := s.index.positionForOffset(offset)
	var out []byte
	var prefix [12]byte

	for pos < s.size && int32(len(out)) < maxBytes {
		if _, err := s.logFile.ReadAt(prefix[:], pos); err != nil {
			if err == io.EOF {
				break
			}
			return out, err
		}
		baseOffset := int64(binary.BigEndian.Uint64(prefix[0:8]))
		length := int32(binary.BigEndian.Uint32(prefix[8:12]))
		if length <= 0 || pos+12+int64(length) > s.size {
			break
		}
		lastOffsetDelta := int32(0)
		var delta [4]byte
		if _, err := s.logFile.ReadAt(delta[:], pos+23); err == nil {
			lastOffsetDelta = int32(binary.BigEndian.Uint32(delta[:]))
		}
		// Determine whether this batch overlaps the requested offset range.
		if baseOffset+int64(lastOffsetDelta) < offset {
			pos += 12 + int64(length)
			continue
		}
		buf := make([]byte, 12+int64(length))
		if _, err := s.logFile.ReadAt(buf, pos); err != nil {
			return out, err
		}
		out = append(out, buf...)
		pos += 12 + int64(length)
	}
	return out, nil
}

// logEndOffset returns the LEO of this segment.
func (s *Segment) logEndOffset() int64 { return s.nextOffset }

// sizeBytes returns the current on-disk log size.
func (s *Segment) sizeBytes() int64 { return s.size }

func (s *Segment) close() error {
	if err := s.logFile.Sync(); err != nil {
		s.logFile.Close()
		return err
	}
	if err := s.index.close(); err != nil {
		s.logFile.Close()
		return err
	}
	return s.logFile.Close()
}
