package storage

import (
	"errors"
	"sync"
)

// ErrOutOfOrderSequence is returned when a produce carries a base sequence that
// is neither the expected next sequence nor a duplicate of the last one.
var ErrOutOfOrderSequence = errors.New("out of order sequence number")

// ProducerSequenceState tracks the last seen sequence for one producer on one
// partition.
type ProducerSequenceState struct {
	LastSequence int32
	LastOffset   int64
	LastEpoch    int16
}

// PartitionIdempotenceTracker validates produce sequence numbers per producer
// ID and deduplicates retried batches (network retries).
type PartitionIdempotenceTracker struct {
	mu             sync.RWMutex
	producerStates map[int64]*ProducerSequenceState
}

// NewPartitionIdempotenceTracker builds an empty tracker.
func NewPartitionIdempotenceTracker() *PartitionIdempotenceTracker {
	return &PartitionIdempotenceTracker{
		producerStates: make(map[int64]*ProducerSequenceState),
	}
}

// ValidateAndAppend validates the sequence for (pid, epoch, seq) and, when
// valid, runs appendFn to persist the batch. Duplicate sequences return the
// previously assigned offset without re-appending.
func (t *PartitionIdempotenceTracker) ValidateAndAppend(pid int64, epoch int16, seq int32, appendFn func() (int64, error)) (int64, error) {
	if pid < 0 {
		// Non-idempotent producer: bypass sequence tracking.
		return appendFn()
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	state, exists := t.producerStates[pid]
	if !exists {
		offset, err := appendFn()
		if err != nil {
			return -1, err
		}
		t.producerStates[pid] = &ProducerSequenceState{
			LastSequence: seq,
			LastOffset:   offset,
			LastEpoch:    epoch,
		}
		return offset, nil
	}

	// Epoch regression or mismatch fences the old producer.
	if epoch < state.LastEpoch {
		return -1, errors.New("producer epoch regression")
	}

	if seq == state.LastSequence+1 {
		offset, err := appendFn()
		if err != nil {
			return -1, err
		}
		state.LastSequence = seq
		state.LastOffset = offset
		state.LastEpoch = epoch
		return offset, nil
	}

	if seq <= state.LastSequence {
		// Duplicate batch (network retry): return the previously assigned
		// offset without re-appending.
		return state.LastOffset, nil
	}

	return -1, ErrOutOfOrderSequence
}
