package storage

import (
	"errors"
	"sync"
)

var ErrOutOfOrderSequence = errors.New("out of order sequence number")

type ProducerSequenceState struct {
	LastSequence int32
	LastOffset   int64
	LastEpoch    int16
}

type PartitionIdempotenceTracker struct {
	mu             sync.RWMutex
	producerStates map[int64]*ProducerSequenceState
}

func NewPartitionIdempotenceTracker() *PartitionIdempotenceTracker {
	return &PartitionIdempotenceTracker{
		producerStates: make(map[int64]*ProducerSequenceState),
	}
}

func (t *PartitionIdempotenceTracker) ValidateAndAppend(pid int64, epoch int16, seq int32, appendFn func() (int64, error)) (int64, error) {
	if pid < 0 {

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

		return state.LastOffset, nil
	}

	return -1, ErrOutOfOrderSequence
}
