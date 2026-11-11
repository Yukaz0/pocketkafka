package fleet

import (
	"testing"
	"time"
)

func TestEnrollNeverExpires(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.enrollWithExpiry("srv-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeToken(tok); err != nil {
		t.Fatalf("never-expiry token must consume: %v", err)
	}
}

func TestListPrunesUsedAndExpired(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tokUsed, _ := r.enrollWithExpiry("s1", time.Minute)
	r.ConsumeToken(tokUsed)
	r.enrollWithExpiry("s2", -time.Minute)
	r.enrollWithExpiry("s3", time.Hour)
	got := r.ListEnrollments()
	if len(got) != 1 || got[0].ServerID != "s3" {
		t.Fatalf("want only s3 pending, got %d", len(got))
	}
}
