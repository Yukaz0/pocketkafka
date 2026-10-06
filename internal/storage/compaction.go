package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// ErrCompactionRunning reports a compaction already in flight for this partition.
var ErrCompactionRunning = errors.New("compaction already running")

const (
	compactTmpDir      = "compact-tmp"
	compactManifest    = "MANIFEST"
	compactStaged      = ".staged"
	compactBatchSize   = 500
	compactBatchBytes  = 1 << 20
	compactReadBytes   = 1 << 20
	compactMaxKeyBytes = 256 << 20
)

// compactionManifest names the files a swap deletes and installs. It is written
// and fsynced before anything is deleted, so a crash mid-swap is finished by
// applyCompactionSwap on the next open.
type compactionManifest struct {
	Old []string `json:"old"`
	New []string `json:"new"`
}

type compactionPlan struct {
	last     map[string]int64
	total    int64
	planned  int64
	keyBytes int64
}

// Compact keeps the most recent record per non-null key, plus every null-key
// record, and rewrites the partition with contiguous offsets from zero. It never
// holds the partition in memory: one map entry per distinct key, one read page at
// a time. Readers keep working; writers pause only for the swap.
func (p *Partition) Compact() error {
	if !p.compacting.CompareAndSwap(false, true) {
		return ErrCompactionRunning
	}
	defer p.compacting.Store(false)

	p.mu.RLock()
	segs, end := p.compactionSource()
	if len(segs) == 0 {
		p.mu.RUnlock()
		return nil
	}
	plan, err := planCompaction(segs, end)
	if err != nil {
		p.mu.RUnlock()
		os.RemoveAll(filepath.Join(p.dir, compactTmpDir))
		return err
	}
	if plan.planned >= plan.total {
		p.mu.RUnlock()
		return nil // nothing superseded
	}
	w, err := stageCompaction(filepath.Join(p.dir, compactTmpDir), p.maxSegmentBytes, p.indexInterval, segs, end, plan)
	p.mu.RUnlock()
	if err != nil {
		os.RemoveAll(filepath.Join(p.dir, compactTmpDir))
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.installCompaction(w, end); err != nil {
		os.RemoveAll(filepath.Join(p.dir, compactTmpDir))
		return err
	}
	return nil
}

// compactionSource returns the segments to read with the log end offset of the
// snapshot. A tiered segment has no local log, so compaction skips the partition
// rather than paying a restore per read.
func (p *Partition) compactionSource() ([]*Segment, int64) {
	segs := make([]*Segment, 0, len(p.closedSegments)+1)
	segs = append(segs, p.closedSegments...)
	if p.activeSegment != nil {
		segs = append(segs, p.activeSegment)
	}
	for _, s := range segs {
		if s.isRemote() {
			return nil, 0
		}
	}
	return segs, p.nextOffset
}

func planCompaction(segs []*Segment, end int64) (*compactionPlan, error) {
	plan := &compactionPlan{last: make(map[string]int64)}
	var idx, nulls int64
	err := forEachBatch(segs, end, func(b *protocol.RecordBatch) error {
		for _, rec := range b.Records {
			if rec.Key == nil {
				nulls++
			} else {
				k := string(rec.Key)
				if _, seen := plan.last[k]; !seen {
					plan.keyBytes += int64(len(k))
					if plan.keyBytes > compactMaxKeyBytes {
						return fmt.Errorf("compaction key state above %d bytes", compactMaxKeyBytes)
					}
				}
				plan.last[k] = idx
			}
			idx++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	plan.total = idx
	plan.planned = nulls + int64(len(plan.last))
	return plan, nil
}

func stageCompaction(tmpDir string, maxSegBytes, indexInterval int64, segs []*Segment, end int64, plan *compactionPlan) (*compactWriter, error) {
	if err := os.RemoveAll(tmpDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return nil, err
	}
	w, err := newCompactWriter(tmpDir, maxSegBytes, indexInterval)
	if err != nil {
		return nil, err
	}
	var idx int64
	err = forEachBatch(segs, end, func(b *protocol.RecordBatch) error {
		for _, rec := range b.Records {
			keep := rec.Key == nil
			if !keep {
				last, ok := plan.last[string(rec.Key)]
				keep = ok && last == idx
			}
			idx++
			if keep {
				if err := w.add(rec); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		w.close()
		return nil, err
	}
	return w, nil
}

// installCompaction runs under the write lock: it carries over what was appended
// while the plan was built, publishes the manifest, then swaps the files.
func (p *Partition) installCompaction(w *compactWriter, snapshotEnd int64) error {
	if p.activeSegment == nil {
		return errors.New("compaction: no active segment")
	}
	if err := forEachBatchRange(p.activeSegment, snapshotEnd, p.nextOffset, func(b *protocol.RecordBatch) error {
		for _, rec := range b.Records {
			if err := w.add(rec); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	names, err := w.finish()
	if err != nil {
		return err
	}
	tmp := filepath.Join(p.dir, compactTmpDir)
	if err := stageSwapFiles(tmp, names); err != nil {
		return err
	}
	old, err := dirFileNames(p.dir)
	if err != nil {
		return err
	}
	if err := writeManifest(tmp, compactionManifest{Old: old, New: names}); err != nil {
		return err
	}
	if err := p.closeSegments(); err != nil {
		return err
	}
	if err := applyCompactionSwap(p.dir); err != nil {
		return err
	}
	if err := p.recover(); err != nil {
		return err
	}
	p.lastCompactMs = time.Now().UnixMilli()
	p.bytesAtCompact = p.totalSizeLocked()
	return nil
}

func (p *Partition) closeSegments() error {
	var firstErr error
	for _, s := range p.orderedSegmentsLocked() {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.closedSegments = nil
	p.activeSegment = nil
	return firstErr
}

// applyCompactionSwap drops the pre-compaction files and moves the staged set in.
// It is idempotent, so recovery runs it again after a crash: a name shared by the
// old and new sets is only removed while its staged replacement is still in the
// staging directory, which is what keeps a partial swap from mixing two offset
// spaces.
func applyCompactionSwap(dir string) error {
	tmp := filepath.Join(dir, compactTmpDir)
	data, err := os.ReadFile(filepath.Join(tmp, compactManifest))
	if err != nil {
		return os.RemoveAll(tmp)
	}
	var m compactionManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return os.RemoveAll(tmp)
	}
	staged := func(name string) string { return filepath.Join(tmp, name+compactStaged) }
	replaced := make(map[string]bool, len(m.New))
	for _, name := range m.New {
		replaced[name] = true
	}
	for _, name := range m.Old {
		if replaced[name] {
			if _, err := os.Stat(staged(name)); err != nil {
				continue // already installed
			}
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	for _, name := range m.New {
		if _, err := os.Stat(staged(name)); err != nil {
			continue
		}
		if err := os.Rename(staged(name), filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(tmp, compactManifest)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	return syncDir(dir)
}

// stageSwapFiles renames the staged segment files aside so the swap can tell an
// installed file from a not-yet-installed one with the same final name.
func stageSwapFiles(tmp string, names []string) error {
	for _, name := range names {
		if err := os.Rename(filepath.Join(tmp, name), filepath.Join(tmp, name+compactStaged)); err != nil {
			return err
		}
	}
	return syncDir(tmp)
}

func writeManifest(tmpDir string, m compactionManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmpDir, compactManifest), data, 0o600); err != nil {
		return err
	}
	return syncDir(tmpDir)
}

func dirFileNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// compactWriter writes retained records into fresh segments in a staging
// directory, rolling at the configured segment size.
type compactWriter struct {
	dir          string
	maxBytes     int64
	interval     int64
	base         int64
	seg          *Segment
	files        []string
	batch        []protocol.Record
	pendingBytes int
}

func newCompactWriter(dir string, maxBytes, interval int64) (*compactWriter, error) {
	w := &compactWriter{dir: dir, maxBytes: maxBytes, interval: interval}
	if err := w.roll(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *compactWriter) roll() error {
	seg, err := openSegment(w.dir, w.base, w.maxBytes, w.interval)
	if err != nil {
		return err
	}
	w.seg = seg
	w.files = append(w.files, filepath.Base(seg.logPath()), filepath.Base(seg.indexPath()))
	return nil
}

// add buffers a record, flushing on whichever bound comes first: the record
// count or the buffered bytes. Bounding by bytes is what keeps a topic of large
// values from buffering hundreds of megabytes per batch.
func (w *compactWriter) add(rec protocol.Record) error {
	w.batch = append(w.batch, protocol.Record{
		OffsetDelta: int32(len(w.batch)),
		Key:         rec.Key,
		Value:       rec.Value,
		Headers:     rec.Headers,
	})
	w.pendingBytes += len(rec.Key) + len(rec.Value)
	if len(w.batch) < compactBatchSize && w.pendingBytes < compactBatchBytes {
		return nil
	}
	return w.flush()
}

func (w *compactWriter) flush() error {
	if len(w.batch) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	raw, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now,
		MaxTimestamp:  now,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records:       w.batch,
	})
	if err != nil {
		return err
	}
	if _, err := w.seg.append(raw); err != nil {
		return err
	}
	w.batch = w.batch[:0]
	w.pendingBytes = 0
	if w.seg.full() {
		w.base = w.seg.nextOffset
		if err := w.seg.close(); err != nil {
			return err
		}
		return w.roll()
	}
	return nil
}

func (w *compactWriter) finish() ([]string, error) {
	if err := w.flush(); err != nil {
		return nil, err
	}
	if err := w.seg.close(); err != nil {
		return nil, err
	}
	if err := syncDir(w.dir); err != nil {
		return nil, err
	}
	return w.files, nil
}

func (w *compactWriter) close() {
	if w.seg != nil {
		w.seg.close()
	}
}

func forEachBatch(segs []*Segment, end int64, fn func(*protocol.RecordBatch) error) error {
	for _, seg := range segs {
		limit := seg.logEndOffset()
		if end > 0 && end < limit {
			limit = end
		}
		if err := forEachBatchRange(seg, seg.baseOffset, limit, fn); err != nil {
			return err
		}
	}
	return nil
}

func forEachBatchRange(seg *Segment, from, to int64, fn func(*protocol.RecordBatch) error) error {
	for next := from; next < to; {
		raw, err := seg.read(next, compactReadBytes)
		if err != nil {
			return err
		}
		advanced := false
		for pos := 0; pos+12 <= len(raw); {
			h, err := ParseRecordBatchHeader(raw[pos:])
			if err != nil {
				break
			}
			total := int(BatchTotalSize(h))
			if total <= 0 || pos+total > len(raw) {
				break
			}
			if b, err := protocol.DecodeRecordBatch(raw[pos : pos+total]); err == nil {
				if err := fn(b); err != nil {
					return err
				}
				advanced = true
				next = int64(binary.BigEndian.Uint64(raw[pos:pos+8])) + int64(h.LastOffsetDelta) + 1
			}
			pos += total
		}
		if !advanced {
			break
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
			if err := p.Compact(); err != nil && !errors.Is(err, ErrCompactionRunning) {
				fmt.Println("compact error", topic.Name, err)
			}
		}
	}
}
