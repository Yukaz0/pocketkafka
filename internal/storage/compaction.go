package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// compactItem is one record considered for compaction.
type compactItem struct {
	key     []byte
	value   []byte
	headers []protocol.RecordHeader
	ts      int64
}

// Compact implements log compaction (cleanup.policy=compact) for a partition:
// it keeps only the most recent record for each non-null key (and all null-key
// records), then rewrites the partition. Offsets are renumbered contiguously,
// which is a simplification of Kafka's offset-preserving compaction.
func (p *Partition) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw, err := p.readAllLocked(1 << 30)
	if err != nil {
		return err
	}

	var items []compactItem
	last := map[string]int{}
	pos := 0
	for pos < len(raw) {
		h, err := ParseRecordBatchHeader(raw[pos:])
		if err != nil {
			break
		}
		total := int(BatchTotalSize(h))
		if pos+total > len(raw) {
			break
		}
		b, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
		if err == nil {
			for _, rec := range b.Records {
				items = append(items, compactItem{key: rec.Key, value: rec.Value, headers: rec.Headers, ts: b.BaseTimestamp + rec.TimestampDelta})
				if rec.Key != nil {
					last[string(rec.Key)] = len(items) - 1
				}
			}
		}
		pos += total
	}

	// Keep the last occurrence of each key, and all null-key records.
	var kept []compactItem
	for i, it := range items {
		if it.key == nil || last[string(it.key)] == i {
			kept = append(kept, it)
		}
	}

	if err := p.wipeLocked(); err != nil {
		return err
	}
	if err := p.appendItemsLocked(kept); err != nil {
		return err
	}
	p.nextOffset = p.activeSegment.nextOffset
	p.highWatermark = p.nextOffset
	return nil
}

// readAllLocked reads every record batch across all segments.
func (p *Partition) readAllLocked(maxBytes int32) ([]byte, error) {
	var out []byte
	for _, seg := range p.orderedSegmentsLocked() {
		data, err := seg.read(seg.baseOffset, maxBytes-int32(len(out)))
		if err != nil {
			return out, err
		}
		out = append(out, data...)
	}
	return out, nil
}

// wipeLocked removes all segment files and resets the partition to an empty
// state with a fresh active segment at offset 0.
func (p *Partition) wipeLocked() error {
	if p.activeSegment != nil {
		p.activeSegment.close()
	}
	for _, seg := range p.closedSegments {
		seg.close()
	}
	p.closedSegments = nil
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if name == "" || name[0] == '.' {
			continue
		}
		if err := os.Remove(filepath.Join(p.dir, name)); err != nil {
			return err
		}
	}
	seg, err := openSegment(p.dir, 0, p.maxSegmentBytes, p.indexInterval)
	if err != nil {
		return err
	}
	p.activeSegment = seg
	p.nextOffset = 0
	p.highWatermark = 0
	return nil
}

// appendItemsLocked appends retained records in contiguous batches.
func (p *Partition) appendItemsLocked(items []compactItem) error {
	const batchSize = 500
	for i := 0; i < len(items); i += batchSize {
		b := &protocol.RecordBatch{
			BaseTimestamp: time.Now().UnixMilli(),
			MaxTimestamp:  time.Now().UnixMilli(),
			ProducerID:    -1,
			ProducerEpoch: -1,
			BaseSequence:  -1,
		}
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		for j := i; j < end; j++ {
			b.Records = append(b.Records, protocol.Record{
				OffsetDelta: int32(j - i),
				Key:         items[j].key,
				Value:       items[j].value,
				Headers:     items[j].headers,
			})
		}
		raw, err := protocol.EncodeRecordBatch(b)
		if err != nil {
			return err
		}
		if _, err := p.activeSegment.append(raw); err != nil {
			return err
		}
	}
	return nil
}

// StartCompaction launches a background goroutine that periodically compacts all
// topics marked with cleanup.policy=compact. It returns a stop function.
func (s *Store) StartCompaction(interval time.Duration) func() {
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.runCompaction()
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop) }
}

func (s *Store) runCompaction() {
	for _, topic := range s.TopicsSnapshot() {
		if !s.IsCompacted(topic.Name) {
			continue
		}
		for _, p := range topic.Partitions {
			if err := p.Compact(); err != nil {
				fmt.Println("compact error", topic.Name, err)
			}
		}
	}
}
