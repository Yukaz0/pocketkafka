package web

import (
	"strconv"
	"testing"
)

// TestCapTailFrame: a live tail under heavy ingest must stay bounded and say how
// much it skipped, keeping the newest records rather than replaying a backlog a
// browser cannot render.
func TestCapTailFrame(t *testing.T) {
	recs := func(n int) []messageRecord {
		out := make([]messageRecord, n)
		for i := range out {
			out[i] = messageRecord{Topic: "orders", Offset: int64(i), Value: strconv.Itoa(i)}
		}
		return out
	}

	// Under the cap: everything is sent, nothing reported as skipped.
	got, skipped := capTailFrame(recs(3), 10)
	if skipped != 0 || len(got) != 3 {
		t.Fatalf("under the cap: len=%d skipped=%d, want 3 and 0", len(got), skipped)
	}

	// Exactly at the cap.
	if got, skipped = capTailFrame(recs(2000), 2000); skipped != 0 || len(got) != 2000 {
		t.Fatalf("at the cap: len=%d skipped=%d, want 2000 and 0", len(got), skipped)
	}

	// Over the cap: keep the newest, report the rest.
	got, skipped = capTailFrame(recs(5000), 2000)
	if skipped != 3000 || len(got) != 2000 {
		t.Fatalf("over the cap: len=%d skipped=%d, want 2000 and 3000", len(got), skipped)
	}
	if got[0].Offset != 3000 || got[len(got)-1].Offset != 4999 {
		t.Fatalf("kept offsets %d..%d, want the newest 3000..4999", got[0].Offset, got[len(got)-1].Offset)
	}

	// A disabled cap leaves the frame untouched.
	if got, skipped = capTailFrame(recs(4), 0); skipped != 0 || len(got) != 4 {
		t.Fatalf("cap 0: len=%d skipped=%d, want 4 and 0", len(got), skipped)
	}
}
