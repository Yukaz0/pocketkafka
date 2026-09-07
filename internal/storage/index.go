package storage

import (
	"encoding/binary"
	"os"
	"sort"
)

// indexEntry is a single 8-byte sparse index entry mapping a relative offset to
// a physical byte position within the associated .log segment.
//
//	+-----------------------------------+-----------------------------------+
//	|   Relative Offset (uint32)        |  Physical Byte Position (uint32)  |
//	|           (4 Bytes)               |              (4 Bytes)            |
//	+-----------------------------------+-----------------------------------+
type indexEntry struct {
	relativeOffset uint32
	position       uint32
}

const indexEntrySize = 8

// indexFile is a memory-mapped style sparse index backed by a file on disk.
// For simplicity and correctness we keep an in-memory copy of the entries and
// flush them to disk as we append. Segments are small enough that the sparse
// index is fully resident in memory, matching the design goal in the spec.
type indexFile struct {
	path        string
	baseOffset  int64
	interval    int64
	file        *os.File
	entries     []indexEntry
	writtenSize int64
}

// openIndex opens (or creates) the sparse index file for a segment.
func openIndex(path string, baseOffset int64, interval int64) (*indexFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	idx := &indexFile{
		path:       path,
		baseOffset: baseOffset,
		interval:   interval,
		file:       f,
	}
	if err := idx.load(); err != nil {
		f.Close()
		return nil, err
	}
	return idx, nil
}

// load reads any existing entries from disk so a reopened segment keeps its
// sparse index (crash recovery).
func (idx *indexFile) load() error {
	st, err := idx.file.Stat()
	if err != nil {
		return err
	}
	if st.Size()%indexEntrySize != 0 {
		// Truncate a partially written tail entry.
		trim := st.Size() / indexEntrySize * indexEntrySize
		if err := idx.file.Truncate(trim); err != nil {
			return err
		}
	}
	buf := make([]byte, st.Size())
	if _, err := idx.file.ReadAt(buf, 0); err != nil {
		return err
	}
	for i := 0; i+indexEntrySize <= len(buf); i += indexEntrySize {
		idx.entries = append(idx.entries, indexEntry{
			relativeOffset: binary.BigEndian.Uint32(buf[i : i+4]),
			position:       binary.BigEndian.Uint32(buf[i+4 : i+8]),
		})
	}
	idx.writtenSize = st.Size()
	return nil
}

// maybeAppend adds a new entry if the segment log position has advanced by at
// least the index interval since the last entry.
func (idx *indexFile) maybeAppend(absoluteOffset int64, position int64) error {
	lastPos := int64(0)
	if len(idx.entries) > 0 {
		lastPos = int64(idx.entries[len(idx.entries)-1].position)
	}
	if position-lastPos < idx.interval {
		return nil
	}
	return idx.append(absoluteOffset, position)
}

func (idx *indexFile) append(absoluteOffset int64, position int64) error {
	rel := uint32(absoluteOffset - idx.baseOffset)
	pos := uint32(position)
	idx.entries = append(idx.entries, indexEntry{relativeOffset: rel, position: pos})
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], rel)
	binary.BigEndian.PutUint32(buf[4:8], pos)
	n, err := idx.file.WriteAt(buf[:], idx.writtenSize)
	if err != nil {
		return err
	}
	idx.writtenSize += int64(n)
	return nil
}

// positionForOffset returns the physical byte position in the .log file that
// corresponds to the largest relative offset <= targetOffset. If the index is
// empty, it returns 0.
func (idx *indexFile) positionForOffset(targetOffset int64) int64 {
	rel := uint32(targetOffset - idx.baseOffset)
	n := len(idx.entries)
	if n == 0 {
		return 0
	}
	// Entries are written in order; binary search for the last rel <= target.
	i := sort.Search(n, func(i int) bool { return idx.entries[i].relativeOffset > rel })
	if i == 0 {
		// Even the first entry is ahead of the target; start from 0.
		return 0
	}
	return int64(idx.entries[i-1].position)
}

// reset clears the in-memory entries and truncates the index file to zero.
func (idx *indexFile) reset() error {
	idx.entries = idx.entries[:0]
	idx.writtenSize = 0
	if idx.file != nil {
		return idx.file.Truncate(0)
	}
	return nil
}

func (idx *indexFile) close() error {
	if err := idx.file.Sync(); err != nil {
		idx.file.Close()
		return err
	}
	return idx.file.Close()
}
