package coordinator

import (
	"sync"
	"sync/atomic"
)

var producerIDGen atomic.Int64

type producerState struct {
	transactionalID string
	epoch           int16
}

type ProducerIDManager struct {
	mu      sync.RWMutex
	byID    map[int64]*producerState
	byTxnID map[string]int64
	lastPID atomic.Int64
}

func NewProducerIDManager() *ProducerIDManager {
	m := &ProducerIDManager{
		byID:    make(map[int64]*producerState),
		byTxnID: make(map[string]int64),
	}
	m.lastPID.Store(1)
	return m
}

func (m *ProducerIDManager) Next(transactionalID *string) (int64, int16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if transactionalID != nil && *transactionalID != "" {
		if pid, ok := m.byTxnID[*transactionalID]; ok {
			st := m.byID[pid]
			st.epoch++
			return pid, st.epoch
		}
	}
	pid := m.lastPID.Add(1)
	m.byID[pid] = &producerState{epoch: 0}
	if transactionalID != nil && *transactionalID != "" {
		m.byID[pid].transactionalID = *transactionalID
		m.byTxnID[*transactionalID] = pid
	}
	return pid, 0
}

func (m *ProducerIDManager) Validate(transactionalID string, pid int64, epoch int16) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.byID[pid]
	if !ok {
		return false
	}
	if transactionalID != "" && st.transactionalID != transactionalID {
		return false
	}
	return st.epoch == epoch
}
