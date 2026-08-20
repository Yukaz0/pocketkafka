package client

import (
	"sync"
	"testing"
	"time"
)

// TestRecordAccumulatorBatching verifies the accumulator flushes full batches
// and drains lingered batches (Fitur 16).
func TestRecordAccumulatorBatching(t *testing.T) {
	var mu sync.Mutex
	var flushes [][]*Message
	acc := NewRecordAccumulator(3, 20*time.Millisecond, func(msgs []*Message) error {
		mu.Lock()
		flushes = append(flushes, msgs)
		mu.Unlock()
		return nil
	})
	defer acc.Close()

	// Fill a batch of 3 -> immediate flush.
	acc.Append(&Message{Topic: "t", Value: []byte("1")})
	acc.Append(&Message{Topic: "t", Value: []byte("2")})
	acc.Append(&Message{Topic: "t", Value: []byte("3")}) // full -> flush

	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	if len(flushes) != 1 || len(flushes[0]) != 3 {
		t.Fatalf("expected 1 flush of 3, got %d flushes (first %d)", len(flushes), len(flushes[0]))
	}
	mu.Unlock()

	// A single message lingers and is flushed by the timer.
	acc.Append(&Message{Topic: "t", Value: []byte("4")})
	time.Sleep(80 * time.Millisecond)
	mu.Lock()
	if len(flushes) != 2 || len(flushes[1]) != 1 {
		t.Fatalf("expected 2nd flush of 1, got %d flushes", len(flushes))
	}
	mu.Unlock()
}

// TestRecordAccumulatorFlush drains all pending batches on Close.
func TestRecordAccumulatorFlush(t *testing.T) {
	var mu sync.Mutex
	var flushes [][]*Message
	acc := NewRecordAccumulator(100, time.Hour, func(msgs []*Message) error {
		mu.Lock()
		flushes = append(flushes, msgs)
		mu.Unlock()
		return nil
	})
	acc.Append(&Message{Topic: "t", Value: []byte("a")})
	acc.Append(&Message{Topic: "t", Value: []byte("b")})
	acc.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(flushes) != 1 || len(flushes[0]) != 2 {
		t.Fatalf("expected 1 final flush of 2, got %d flushes", len(flushes))
	}
}
