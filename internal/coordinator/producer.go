package coordinator

import (
	"sync"
	"sync/atomic"
)

// producerIDGen allocates unique producer IDs for idempotent producers.
var producerIDGen atomic.Int64

// producerState tracks the epoch and transactional mapping for one producer ID.
type producerState struct {
	transactionalID string
	epoch           int16
}

// ProducerIDManager allocates and validates producer IDs/epochs used by the
// idempotent producer and transactional producer APIs (Keys 22, 24, 25, 26).
type ProducerIDManager struct {
	mu      sync.RWMutex
	byID    map[int64]*producerState
	byTxnID map[string]int64
	lastPID atomic.Int64
}

// NewProducerIDManager builds an empty manager. IDs start at 1 (0 is reserved
// for unknown producers in the Kafka protocol).
func NewProducerIDManager() *ProducerIDManager {
	m := &ProducerIDManager{
		byID:    make(map[int64]*producerState),
		byTxnID: make(map[string]int64),
	}
	m.lastPID.Store(1)
	return m
}

// Next allocates a fresh producer ID (and epoch 0) for a client. When a
// transactional ID is supplied and already known, the same producer ID is
// returned with a bumped epoch (re-initialization).
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

// Validate reports whether the (transactionalID, producerID, epoch) triple is
// known and matches the coordinator's mapping.
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
