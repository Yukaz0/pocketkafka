package storage

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Partition struct {
	topic       string
	partitionID int32

	mu              sync.RWMutex
	dir             string
	activeSegment   *Segment
	closedSegments  []*Segment
	nextOffset      int64
	highWatermark   int64
	maxSegmentBytes int64
	indexInterval   int64

	idempotence *PartitionIdempotenceTracker

	appendMu sync.Mutex
	appendCh chan struct{}

	remoteFetch func(key string) ([]byte, error)

	remoteDelete func(logKey, indexKey string) error

	compacting atomic.Bool

	lastCompactMs  int64
	bytesAtCompact int64
}

func OpenPartition(dir, topic string, partitionID int32, maxSegmentBytes, indexInterval int64) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create partition dir: %w", err)
	}
	p := &Partition{
		topic:           topic,
		partitionID:     partitionID,
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		indexInterval:   indexInterval,
		idempotence:     NewPartitionIdempotenceTracker(),
		appendCh:        make(chan struct{}),
	}
	if err := applyCompactionSwap(dir); err != nil {
		return nil, fmt.Errorf("recover compaction: %w", err)
	}
	if err := p.recover(); err != nil {
		return nil, err
	}
	return p, nil
}

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

func (p *Partition) DataWaiter() (<-chan struct{}, int64) {
	p.appendMu.Lock()
	ch := p.appendCh
	p.appendMu.Unlock()

	p.mu.RLock()
	hwm := p.highWatermark
	p.mu.RUnlock()
	return ch, hwm
}

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

	p.appendMu.Lock()
	close(p.appendCh)
	p.appendCh = make(chan struct{})
	p.appendMu.Unlock()
	return assigned, nil
}

func (p *Partition) AppendIdempotent(pid int64, epoch int16, baseSeq int32, raw []byte) (int64, error) {
	return p.AppendIdempotentSynced(pid, epoch, baseSeq, raw, false)
}

func (p *Partition) AppendSynced(raw []byte, sync bool) (int64, error) {
	off, err := p.Append(raw)
	if err != nil {
		return 0, err
	}
	if sync {
		if err := p.Sync(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

func (p *Partition) AppendIdempotentSynced(pid int64, epoch int16, baseSeq int32, raw []byte, sync bool) (int64, error) {
	return p.idempotence.ValidateAndAppend(pid, epoch, baseSeq, func() (int64, error) {
		return p.AppendSynced(raw, sync)
	})
}

func (p *Partition) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeSegment != nil {
		if p.activeSegment.syncedBytes == p.activeSegment.size {
			return nil
		}
		return p.activeSegment.sync()
	}
	return nil
}

func (p *Partition) TruncateTo(target int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if target <= 0 {
		target = 0
	}
	if target >= p.nextOffset {
		return nil
	}

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

type SegmentInfo struct {
	BaseOffset int64
	SizeBytes  int64
	Remote     bool
}

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

func (p *Partition) roll() error {
	p.closedSegments = append(p.closedSegments, p.activeSegment)
	seg, err := openSegment(p.dir, p.nextOffset, p.maxSegmentBytes, p.indexInterval)
	if err != nil {
		return err
	}
	p.activeSegment = seg
	return nil
}

func (p *Partition) Read(startOffset int64, maxBytes int32) ([]byte, int64, error) {
	if startOffset < 0 {
		startOffset = 0
	}

	p.mu.RLock()
	if startOffset >= p.highWatermark {
		hwm := p.highWatermark
		p.mu.RUnlock()
		return nil, hwm, nil
	}
	restore := false
	for _, seg := range p.orderedSegmentsLocked() {
		if seg.logEndOffset() > startOffset && seg.isRemote() {
			restore = true
			break
		}
	}
	if !restore {
		out, hwm, err := p.readLocked(startOffset, maxBytes)
		p.mu.RUnlock()
		return out, hwm, err
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.orderedSegmentsLocked() {
		if seg.logEndOffset() > startOffset && seg.isRemote() {
			if err := p.restoreSegmentLocked(seg); err != nil {
				return nil, p.highWatermark, err
			}
		}
	}
	return p.readLocked(startOffset, maxBytes)
}

func (p *Partition) readLocked(startOffset int64, maxBytes int32) ([]byte, int64, error) {
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
			return out, p.highWatermark, fmt.Errorf("segment %d is remote and was not restored", segs[idx].baseOffset)
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

func (p *Partition) orderedSegmentsLocked() []*Segment {
	segs := make([]*Segment, 0, len(p.closedSegments)+1)
	segs = append(segs, p.closedSegments...)
	if p.activeSegment != nil {
		segs = append(segs, p.activeSegment)
	}
	return segs
}

func (p *Partition) GetOffset(target int64) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch target {
	case -1:
		return p.highWatermark, nil
	case -2:
		return p.earliestOffsetLocked(), nil
	default:

		return -1, nil
	}
}

func (p *Partition) earliestOffsetLocked() int64 {
	if len(p.closedSegments) > 0 {
		return p.closedSegments[0].baseOffset
	}
	if p.activeSegment != nil {
		return p.activeSegment.baseOffset
	}
	return p.nextOffset
}

func (p *Partition) PartitionID() int32 { return p.partitionID }

func (p *Partition) LogEndOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.highWatermark
}

func (p *Partition) EarliestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.earliestOffsetLocked()
}

func (p *Partition) Durability() (durableOffset, bytesAtRisk, lastSyncMs int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.activeSegment == nil {
		return p.nextOffset, 0, 0
	}
	risk := p.activeSegment.size - p.activeSegment.syncedBytes
	if risk < 0 {
		risk = 0
	}
	return p.activeSegment.syncedOffset, risk, p.activeSegment.lastSyncMs
}

func (p *Partition) CompactionDebt() (bytes int64, lastCompactMs int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	base := int64(0)
	if p.lastCompactMs > 0 {
		base = p.bytesAtCompact
	}
	if debt := p.totalSizeLocked() - base; debt > 0 {
		return debt, p.lastCompactMs
	}
	return 0, p.lastCompactMs
}

func (p *Partition) SizeBytes() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalSizeLocked()
}

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

func (p *Partition) Dir() string { return p.dir }

func (p *Partition) Delete() error {
	if err := p.Close(); err != nil {
		return err
	}
	return os.RemoveAll(p.dir)
}
