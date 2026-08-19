package storage

import (
	"os"
	"time"
)

// Retention configures the background cleanup policy.
type Retention struct {
	CheckInterval  time.Duration
	RetentionTime  time.Duration // max segment age before deletion
	RetentionBytes int64         // max total log size; -1 for unlimited
}

// StartRetention launches a background goroutine that periodically enforces the
// retention policy across all partitions. It returns a stop function.
func (s *Store) StartRetention(ret Retention) func() {
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(ret.CheckInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.enforceRetention(ret)
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop) }
}

func (s *Store) enforceRetention(ret Retention) {
	for _, topic := range s.TopicsSnapshot() {
		for _, p := range topic.Partitions {
			p.enforceRetention(ret)
		}
	}
}

// enforceRetention removes closed segments that are too old or push total size
// above the retention ceiling. The active segment is never deleted.
func (p *Partition) enforceRetention(ret Retention) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Time based: drop the oldest closed segment if it is older than the limit.
	if ret.RetentionTime > 0 {
		cutoff := time.Now().Add(-ret.RetentionTime)
		for len(p.closedSegments) > 0 {
			seg := p.closedSegments[0]
			if mtime, err := fileMtime(seg.logFile.Name()); err == nil && mtime.Before(cutoff) {
				p.removeClosedSegment(0)
				continue
			}
			break
		}
	}

	// Size based: delete oldest closed segments while total size exceeds the cap.
	if ret.RetentionBytes > 0 {
		for p.totalSizeLocked() > ret.RetentionBytes && len(p.closedSegments) > 0 {
			p.removeClosedSegment(0)
		}
	}
}

// totalSizeLocked sums the on-disk size of all segments in the partition.
func (p *Partition) totalSizeLocked() int64 {
	var total int64
	for _, seg := range p.closedSegments {
		total += seg.sizeBytes()
	}
	if p.activeSegment != nil {
		total += p.activeSegment.sizeBytes()
	}
	return total
}

// removeClosedSegment deletes the closed segment at index i from memory and disk.
func (p *Partition) removeClosedSegment(i int) {
	seg := p.closedSegments[i]
	name := seg.logFile.Name()
	seg.close()
	os.Remove(name) // .log
	os.Remove(trimSuffix(name, ".log") + ".index")
	p.closedSegments = append(p.closedSegments[:i], p.closedSegments[i+1:]...)
}

func fileMtime(path string) (time.Time, error) {
	st, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return st.ModTime(), nil
}

func trimSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}
