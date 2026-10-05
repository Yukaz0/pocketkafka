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
// retention policy across all partitions. It returns a stop function. The policy
// is kept so the dashboard can report what a pass would delete.
func (s *Store) StartRetention(ret Retention) func() {
	s.retentionMu.Lock()
	s.ret = &ret
	s.retentionMu.Unlock()
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
	plan := p.retentionPlanLocked(ret)
	// Newest index first, so the remaining indices stay valid.
	for i := len(plan) - 1; i >= 0; i-- {
		p.removeClosedSegment(plan[i])
	}
}

// retentionPlanLocked lists the closed segments a pass would drop, oldest first,
// without touching anything. The decision and the deletion share this function
// so the dashboard can report policy debt from the same rule that deletes.
func (p *Partition) retentionPlanLocked(ret Retention) []int {
	var plan []int
	total := p.totalSizeLocked()
	if ret.RetentionTime > 0 {
		cutoff := time.Now().Add(-ret.RetentionTime)
		for i, seg := range p.closedSegments {
			mtime, err := seg.modTime()
			if err != nil || !mtime.Before(cutoff) {
				break
			}
			plan = append(plan, i)
			total -= seg.sizeBytes()
		}
	}
	if ret.RetentionBytes > 0 {
		for i := len(plan); i < len(p.closedSegments) && total > ret.RetentionBytes; i++ {
			plan = append(plan, i)
			total -= p.closedSegments[i].sizeBytes()
		}
	}
	return plan
}

// RetentionDebt reports the bytes a retention pass would delete right now: data
// the policy has already given up on, still occupying disk between passes.
func (p *Partition) RetentionDebt(ret Retention) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var debt int64
	for _, i := range p.retentionPlanLocked(ret) {
		debt += p.closedSegments[i].sizeBytes()
	}
	return debt
}

// Retention returns the active cleanup policy, or ok=false when none was set.
func (s *Store) Retention() (Retention, bool) {
	s.retentionMu.RLock()
	defer s.retentionMu.RUnlock()
	if s.ret == nil {
		return Retention{}, false
	}
	return *s.ret, true
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
// It never touches seg.logFile: an offloaded segment has none, and its local
// files are rebuilt from the .remote stub if it is ever needed again.
func (p *Partition) removeClosedSegment(i int) {
	seg := p.closedSegments[i]
	if seg.isRemote() {
		if logKey, indexKey, err := seg.remoteKeys(); err == nil && p.remoteDelete != nil {
			_ = p.remoteDelete(logKey, indexKey) // best-effort
		}
		if seg.remoteStub != "" {
			os.Remove(seg.remoteStub)
		}
	} else {
		seg.close()
	}
	os.Remove(seg.logPath())
	os.Remove(seg.indexPath())
	p.closedSegments = append(p.closedSegments[:i], p.closedSegments[i+1:]...)
}

func fileMtime(path string) (time.Time, error) {
	st, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return st.ModTime(), nil
}
