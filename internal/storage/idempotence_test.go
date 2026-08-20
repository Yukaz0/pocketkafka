package storage

import "testing"

func TestIdempotenceTracker(t *testing.T) {
	tr := NewPartitionIdempotenceTracker()
	next := int64(100)

	appendFn := func() (int64, error) {
		off := next
		next++
		return off, nil
	}

	// First produce from a PID is accepted.
	off, err := tr.ValidateAndAppend(1, 0, 0, appendFn)
	if err != nil || off != 100 {
		t.Fatalf("first append: off=%d err=%v", off, err)
	}

	// Expected next sequence accepted.
	off, err = tr.ValidateAndAppend(1, 0, 1, appendFn)
	if err != nil || off != 101 {
		t.Fatalf("second append: off=%d err=%v", off, err)
	}

	// Duplicate sequence returns the previous offset without appending.
	off, err = tr.ValidateAndAppend(1, 0, 1, appendFn)
	if err != nil || off != 101 {
		t.Fatalf("duplicate: off=%d err=%v", off, err)
	}
	if next != 102 {
		t.Fatalf("duplicate must not append; next=%d", next)
	}

	// Out-of-order sequence is rejected.
	if _, err := tr.ValidateAndAppend(1, 0, 5, appendFn); err != ErrOutOfOrderSequence {
		t.Fatalf("expected ErrOutOfOrderSequence got %v", err)
	}

	// Epoch regression is rejected.
	if _, err := tr.ValidateAndAppend(1, -1, 2, appendFn); err == nil {
		t.Fatal("expected epoch regression error")
	}

	// Non-idempotent producers bypass tracking entirely.
	off, err = tr.ValidateAndAppend(-1, -1, -1, appendFn)
	if err != nil || off != 102 {
		t.Fatalf("non-idempotent: off=%d err=%v", off, err)
	}
}

func TestIdempotenceSeparateProducers(t *testing.T) {
	tr := NewPartitionIdempotenceTracker()
	next := int64(0)
	appendFn := func() (int64, error) {
		off := next
		next++
		return off, nil
	}
	// Two producers interleave without clobbering each other.
	for i := 0; i < 5; i++ {
		if _, err := tr.ValidateAndAppend(10, 0, int32(i), appendFn); err != nil {
			t.Fatalf("pid 10 seq %d: %v", i, err)
		}
		if _, err := tr.ValidateAndAppend(11, 0, int32(i), appendFn); err != nil {
			t.Fatalf("pid 11 seq %d: %v", i, err)
		}
	}
}
