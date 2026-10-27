package storage

import (
	"os"
	"time"
)

type Retention struct {
	CheckInterval  time.Duration
	RetentionTime  time.Duration
	RetentionBytes int64
}

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

func (p *Partition) enforceRetention(ret Retention) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan := p.retentionPlanLocked(ret)

	for i := len(plan) - 1; i >= 0; i-- {
		p.removeClosedSegment(plan[i])
	}
}

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

func (p *Partition) RetentionDebt(ret Retention) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var debt int64
	for _, i := range p.retentionPlanLocked(ret) {
		debt += p.closedSegments[i].sizeBytes()
	}
	return debt
}

func (s *Store) Retention() (Retention, bool) {
	s.retentionMu.RLock()
	defer s.retentionMu.RUnlock()
	if s.ret == nil {
		return Retention{}, false
	}
	return *s.ret, true
}

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

func (p *Partition) removeClosedSegment(i int) {
	seg := p.closedSegments[i]
	if seg.isRemote() {
		if logKey, indexKey, err := seg.remoteKeys(); err == nil && p.remoteDelete != nil {
			_ = p.remoteDelete(logKey, indexKey)
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
