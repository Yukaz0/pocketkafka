package storage

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

// Partition is a single topic-partition backed by a directory of log segments.
// It serializes appends under a write lock while allowing concurrent reads
// under a read lock, matching the design in docs/ARCHITECTURE_PLAN.md section 3.3.
type Partition struct {
	topic       string
	partitionID int32

	mu              sync.RWMutex
	dir             string
	activeSegment   *Segment
	closedSegments  []*Segment // sorted ascending by base offset
	nextOffset      int64      // Log End Offset (LEO)
	highWatermark   int64      // HWM: offset safe for consumers to read
	maxSegmentBytes int64
	indexInterval   int64

	// idempotence tracks per-producer sequence numbers when idempotent produce
	// is enabled.
	idempotence *PartitionIdempotenceTracker

	// remoteFetch downloads an object-store key (set when tiering is enabled).
	remoteFetch func(key string) ([]byte, error)
}

// OpenPartition opens (or creates) a partition directory and recovers its state.
func OpenPartition(dir, topic string, partitionID int32, maxSegmentBytes, indexInterval int64) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create partition dir: %w", err)
	}
	p := &Partition{
		topic:           topic,
		partitionID:     partitionID,
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		indexInterval:   indexInterval,
		idempotence:     NewPartitionIdempotenceTracker(),
	}
	if err := p.recover(); err != nil {
		return nil, err
	}
	return p, nil
}

// recover scans the partition directory, loads all segments, and restores
// nextOffset / highWatermark. On a fresh directory it creates the first segment.
func (p *Partition) recover() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	var bases []int64
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".remote") {
			var base int64
			if _, err := fmt.Sscanf(name, "%020d.log", &base); err == nil {
				bases = append(bases, base)
			} else if _, err := fmt.Sscanf(name, "%020d.remote", &base); err == nil {
				bases = append(bases, base)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	// De-duplicate base offsets (a segment may have both a .log and a .remote).
	uniq := bases[:0]
	for i, b := range bases {
		if i == 0 || b != bases[i-1] {
			uniq = append(uniq, b)
		}
	}
	for _, base := range uniq {
		seg, err := openSegment(p.dir, base, p.maxSegmentBytes, p.indexInterval)
		if err != nil {
			return err
		}
		p.closedSegments = append(p.closedSegments, seg)
	}
	if len(p.closedSegments) > 0 {
		// The highest-base segment becomes active; the rest stay closed.
		p.activeSegment = p.closedSegments[len(p.closedSegments)-1]
		p.closedSegments = p.closedSegments[:len(p.closedSegments)-1]
	} else {
		seg, err := openSegment(p.dir, 0, p.maxSegmentBytes, p.indexInterval)
		if err != nil {
			return err
		}
		p.activeSegment = seg
	}
	p.nextOffset = p.activeSegment.nextOffset
	p.highWatermark = p.nextOffset
	return nil
}

// Append writes a raw RecordBatch, assigns it the next offset, and advances the
// LEO and HWM. It returns the assigned base offset.
func (p *Partition) Append(raw []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeSegment.full() {
		if err := p.roll(); err != nil {
			return 0, err
		}
	}
	assigned, err := p.activeSegment.append(raw)
	if err != nil {
		return 0, err
	}
	p.nextOffset = p.activeSegment.nextOffset
	p.highWatermark = p.nextOffset
	return assigned, nil
}

// AppendIdempotent validates the producer sequence for (pid, epoch, baseSeq)
// and appends the batch only when the sequence advances by one. Duplicate
// sequences return the previously assigned offset without persisting again.
func (p *Partition) AppendIdempotent(pid int64, epoch int16, baseSeq int32, raw []byte) (int64, error) {
	return p.idempotence.ValidateAndAppend(pid, epoch, baseSeq, func() (int64, error) {
		return p.Append(raw)
	})
}

// TruncateTo truncates the log to the given offset (inclusive of earlier data),
// dropping all records at or after the target offset. It is used by the admin
// reset-offset / truncate APIs.
func (p *Partition) TruncateTo(target int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if target <= 0 {
		target = 0
	}
	if target >= p.nextOffset {
		return nil
	}

	// Drop segments whose entire range starts at or after the target.
	kept := p.closedSegments[:0]
	for _, seg := range p.closedSegments {
		if seg.baseOffset >= target {
			if err := seg.deleteFiles(); err != nil {
				return err
			}
			continue
		}
		kept = append(kept, seg)
	}
	p.closedSegments = kept

	if p.activeSegment != nil && p.activeSegment.baseOffset >= target {
		if err := p.activeSegment.deleteFiles(); err != nil {
			return err
		}
		// Reopen the newest kept segment as active (or a fresh one at target).
		if len(p.closedSegments) > 0 {
			last := p.closedSegments[len(p.closedSegments)-1]
			p.closedSegments = p.closedSegments[:len(p.closedSegments)-1]
			p.activeSegment = last
		} else {
			seg, err := openSegment(p.dir, 0, p.maxSegmentBytes, p.indexInterval)
			if err != nil {
				return err
			}
			p.activeSegment = seg
		}
	}

	if err := p.activeSegment.truncateTo(target); err != nil {
		return err
	}
	p.nextOffset = target
	p.highWatermark = target
	return nil
}

// SegmentInfo describes one on-disk log segment for the topic detail UI.
type SegmentInfo struct {
	BaseOffset int64
	SizeBytes  int64
	Remote     bool
}

// SegmentInfos returns metadata for every segment of the partition.
func (p *Partition) SegmentInfos() []SegmentInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	segs := p.orderedSegmentsLocked()
	out := make([]SegmentInfo, 0, len(segs))
	for _, s := range segs {
		out = append(out, SegmentInfo{
			BaseOffset: s.baseOffset,
			SizeBytes:  s.sizeBytes(),
			Remote:     s.isRemote(),
		})
	}
	return out
}

// roll closes the active segment and starts a new one at the current LEO. The
// rolled segment stays open for reads until the partition is closed.
func (p *Partition) roll() error {
	p.closedSegments = append(p.closedSegments, p.activeSegment)
	seg, err := openSegment(p.dir, p.nextOffset, p.maxSegmentBytes, p.indexInterval)
	if err != nil {
		return err
	}
	p.activeSegment = seg
	return nil
}

// Read returns raw RecordBatch bytes starting at startOffset up to maxBytes
// along with the current high watermark. It reads across segment boundaries.
func (p *Partition) Read(startOffset int64, maxBytes int32) ([]byte, int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if startOffset < 0 {
		startOffset = 0
	}
	if startOffset >= p.highWatermark {
		return nil, p.highWatermark, nil
	}

	segs := p.orderedSegmentsLocked()
	idx := 0
	for ; idx < len(segs); idx++ {
		if startOffset < segs[idx].logEndOffset() {
			break
		}
	}

	var out []byte
	for ; idx < len(segs); idx++ {
		if int32(len(out)) >= maxBytes {
			break
		}
		if segs[idx].isRemote() {
			if err := p.restoreSegmentLocked(segs[idx]); err != nil {
				return out, p.highWatermark, err
			}
		}
		data, err := segs[idx].read(startOffset, maxBytes-int32(len(out)))
		if err != nil {
			return out, p.highWatermark, err
		}
		out = append(out, data...)
		startOffset = segs[idx].logEndOffset()
	}
	return out, p.highWatermark, nil
}

// restoreSegmentLocked downloads an offloaded segment's data and restores it.
func (p *Partition) restoreSegmentLocked(seg *Segment) error {
	if p.remoteFetch == nil {
		return fmt.Errorf("remote segment %d has no fetcher configured", seg.baseOffset)
	}
	logKey, indexKey, err := seg.remoteKeys()
	if err != nil {
		return err
	}
	logData, err := p.remoteFetch(logKey)
	if err != nil {
		return err
	}
	indexData, err := p.remoteFetch(indexKey)
	if err != nil {
		return err
	}
	return seg.restoreFrom(logData, indexData)
}

// orderedSegmentsLocked returns the segments in offset order (closed then active).
func (p *Partition) orderedSegmentsLocked() []*Segment {
	segs := make([]*Segment, 0, len(p.closedSegments)+1)
	segs = append(segs, p.closedSegments...)
	if p.activeSegment != nil {
		segs = append(segs, p.activeSegment)
	}
	return segs
}

// GetOffset returns the earliest (-2) or latest (-1) offset for the partition.
func (p *Partition) GetOffset(target int64) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch target {
	case -1: // Latest
		return p.highWatermark, nil
	case -2: // Earliest
		return p.earliestOffsetLocked(), nil
	default:
		// Approximate: a timestamp we cannot index maps to the LEO.
		return -1, nil
	}
}

// earliestOffsetLocked returns the first offset available in the oldest segment.
func (p *Partition) earliestOffsetLocked() int64 {
	if len(p.closedSegments) > 0 {
		return p.closedSegments[0].baseOffset
	}
	if p.activeSegment != nil {
		return p.activeSegment.baseOffset
	}
	return p.nextOffset
}

// LogEndOffset returns the LEO of the partition.
func (p *Partition) LogEndOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

// HighWatermark returns the current HWM.
func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.highWatermark
}

// EarliestOffset returns the first available offset in the partition.
func (p *Partition) EarliestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.earliestOffsetLocked()
}

// SizeBytes returns the total on-disk size of the partition in bytes.
func (p *Partition) SizeBytes() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalSizeLocked()
}

// Close closes all segment file descriptors.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeSegment != nil {
		p.activeSegment.close()
	}
	for _, s := range p.closedSegments {
		s.close()
	}
	return nil
}

// Dir returns the partition directory path.
func (p *Partition) Dir() string { return p.dir }

// Delete removes the partition directory and all its files from disk.
func (p *Partition) Delete() error {
	if err := p.Close(); err != nil {
		return err
	}
	return os.RemoveAll(p.dir)
}
