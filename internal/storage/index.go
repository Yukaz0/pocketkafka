package storage

import (
	"encoding/binary"
	"os"
	"sort"
)

type indexEntry struct {
	relativeOffset uint32
	position       uint32
}

const indexEntrySize = 8

type indexFile struct {
	path        string
	baseOffset  int64
	interval    int64
	file        *os.File
	entries     []indexEntry
	writtenSize int64
}

func openIndex(path string, baseOffset int64, interval int64) (*indexFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
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

func (idx *indexFile) load() error {
	st, err := idx.file.Stat()
	if err != nil {
		return err
	}
	if st.Size()%indexEntrySize != 0 {

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

func (idx *indexFile) positionForOffset(targetOffset int64) int64 {
	rel := uint32(targetOffset - idx.baseOffset)
	n := len(idx.entries)
	if n == 0 {
		return 0
	}

	i := sort.Search(n, func(i int) bool { return idx.entries[i].relativeOffset > rel })
	if i == 0 {

		return 0
	}
	return int64(idx.entries[i-1].position)
}

func (idx *indexFile) reset() error {
	idx.entries = idx.entries[:0]
	idx.writtenSize = 0
	if idx.file != nil {
		return idx.file.Truncate(0)
	}
	return nil
}

func (idx *indexFile) sync() error {
	if idx.file == nil {
		return nil
	}
	return idx.file.Sync()
}

func (idx *indexFile) close() error {
	if err := idx.file.Sync(); err != nil {
		idx.file.Close()
		return err
	}
	return idx.file.Close()
}
