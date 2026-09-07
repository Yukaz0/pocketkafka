package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	remote          bool   // true when this segment is offloaded to object storage
	remoteStub      string // path to the .remote pointer file
}

// openSegment opens (or creates) a segment whose base offset is baseOffset and
// whose files live in dir. If an existing segment is found it is resumed, which
// recovers nextOffset and size from disk.
func openSegment(dir string, baseOffset int64, maxSegmentBytes, indexInterval int64) (*Segment, error) {
	name := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, name+".log")
	idxPath := filepath.Join(dir, name+".index")
	remotePath := filepath.Join(dir, name+".remote")

	// If the segment has been offloaded, there is no local .log; it will be
	// restored on demand before reads.
	if _, err := os.Stat(remotePath); err == nil {
		_, _, next, err := readRemoteStub(remotePath)
		if err != nil {
			return nil, err
		}
		return &Segment{
			baseOffset:      baseOffset,
			nextOffset:      next,
			dir:             dir,
			maxSegmentBytes: maxSegmentBytes,
			indexInterval:   indexInterval,
			remote:          true,
			remoteStub:      remotePath,
		}, nil
	}

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
		remoteStub:      remotePath,
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

// truncateTo truncates the segment file to the first byte of the batch that
// contains the target offset, dropping all data at or after it. The index is
// rebuilt from the remaining log.
func (s *Segment) truncateTo(target int64) error {
	if target <= s.baseOffset {
		// Everything is after the target: empty the segment.
		if s.logFile != nil {
			if err := s.logFile.Truncate(0); err != nil {
				return err
			}
		}
		s.size = 0
		s.nextOffset = s.baseOffset
		if s.index != nil {
			s.index.reset()
		}
		return nil
	}
	pos := s.index.positionForOffset(target)
	var prefix [12]byte
	for pos < s.size {
		if _, err := s.logFile.ReadAt(prefix[:], pos); err != nil {
			return err
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
		if baseOffset+int64(lastOffsetDelta) >= target {
			break // this batch contains the target; truncate here
		}
		pos += 12 + int64(length)
	}
	if err := s.logFile.Truncate(pos); err != nil {
		return err
	}
	s.size = pos
	s.nextOffset = target
	if s.index != nil {
		s.index.reset()
		// Rebuild a sparse index over the remaining log.
		scan := int64(0)
		rebuild := int64(0)
		for scan < s.size {
			if _, err := s.logFile.ReadAt(prefix[:], scan); err != nil {
				break
			}
			baseOffset := int64(binary.BigEndian.Uint64(prefix[0:8]))
			length := int32(binary.BigEndian.Uint32(prefix[8:12]))
			if length <= 0 || scan+12+int64(length) > s.size {
				break
			}
			s.index.maybeAppend(baseOffset, scan)
			scan += 12 + int64(length)
			rebuild = scan
		}
		_ = rebuild
	}
	return nil
}

// deleteFiles removes the segment's on-disk files (.log, .index, .remote).
func (s *Segment) deleteFiles() error {
	if s.logFile != nil {
		s.logFile.Close()
	}
	if s.index != nil {
		s.index.close()
	}
	os.Remove(s.logPath())
	os.Remove(s.indexPath())
	if s.remoteStub != "" {
		os.Remove(s.remoteStub)
	}
	return nil
}

// sizeBytes returns the current on-disk log size.
func (s *Segment) sizeBytes() int64 { return s.size }

// isRemote reports whether the segment is offloaded to object storage.
func (s *Segment) isRemote() bool { return s.remote }

// markRemote writes a .remote pointer file and releases the local segment files.
// The stub stores the log/index keys and the segment's nextOffset so LEO can be
// recovered after a restart.
func (s *Segment) markRemote(logKey, indexKey string) error {
	content := fmt.Sprintf("%s\n%s\n%d", logKey, indexKey, s.nextOffset)
	if err := os.WriteFile(s.remoteStub, []byte(content), 0o644); err != nil {
		return err
	}
	if s.logFile != nil {
		s.logFile.Close()
	}
	if s.index != nil {
		s.index.close()
	}
	s.logFile = nil
	s.index = nil
	s.size = 0
	s.remote = true
	os.Remove(trimSuffix(s.logPath(), ".log"))
	os.Remove(trimSuffix(s.indexPath(), ".index"))
	return nil
}

// remoteKeys returns the object store keys for the offloaded segment.
func (s *Segment) remoteKeys() (string, string, error) {
	data, err := os.ReadFile(s.remoteStub)
	if err != nil {
		return "", "", err
	}
	lines := strings.SplitN(strings.TrimSpace(string(data)), "\n", 3)
	if len(lines) < 2 {
		return "", "", fmt.Errorf("malformed remote stub %s", s.remoteStub)
	}
	return lines[0], lines[1], nil
}

// readRemoteStub parses a .remote stub, returning keys and the segment LEO.
func readRemoteStub(path string) (string, string, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", 0, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 3 {
		return "", "", 0, fmt.Errorf("malformed remote stub %s", path)
	}
	var next int64
	fmt.Sscanf(lines[2], "%d", &next)
	return lines[0], lines[1], next, nil
}

// restoreFrom writes the downloaded .log and .index back and reopens the segment.
func (s *Segment) restoreFrom(logData, indexData []byte) error {
	if err := os.WriteFile(s.logPath(), logData, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(s.indexPath(), indexData, 0o644); err != nil {
		return err
	}
	os.Remove(s.remoteStub)
	seg, err := openSegment(s.dir, s.baseOffset, s.maxSegmentBytes, s.indexInterval)
	if err != nil {
		return err
	}
	*s = *seg
	return nil
}

func (s *Segment) logPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d.log", s.baseOffset))
}

func (s *Segment) indexPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d.index", s.baseOffset))
}

func (s *Segment) close() error {
	if s.logFile == nil {
		return nil
	}
	if err := s.logFile.Sync(); err != nil {
		s.logFile.Close()
		return err
	}
	if s.index != nil {
		if err := s.index.close(); err != nil {
			s.logFile.Close()
			return err
		}
	}
	return s.logFile.Close()
}
