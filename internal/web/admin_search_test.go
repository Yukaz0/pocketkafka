package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// searchResponse mirrors the fields the search endpoint returns. Only the ones
// these tests assert on are decoded, so adding fields to the handler does not
// break them.
type searchResponse struct {
	Records        []json.RawMessage `json:"records"`
	Scanned        int64             `json:"scanned"`
	Count          int               `json:"count"`
	NextOffset     int64             `json:"nextOffset"`
	BytesRead      int64             `json:"bytesRead"`
	BudgetBytes    int64             `json:"budgetBytes"`
	Truncated      bool              `json:"truncated"`
	StopReason     string            `json:"stopReason"`
	RemoteSegments int               `json:"remoteSegments"`
}

// appendSearchableMessages writes n single-record batches whose values differ
// ("msg-00000", "msg-00001", ...), so a predicate can be selective. appendMessages
// in monitor_test.go writes identical values, which cannot express "matches only
// the last record".
func appendSearchableMessages(t *testing.T, p *storage.Partition, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		now := time.Now().UnixMilli()
		batch := &protocol.RecordBatch{
			BaseTimestamp: now,
			MaxTimestamp:  now,
			ProducerID:    -1,
			ProducerEpoch: -1,
			BaseSequence:  -1,
			Records:       []protocol.Record{{Key: []byte("k"), Value: []byte(fmt.Sprintf("msg-%05d", i))}},
		}
		raw, err := protocol.EncodeRecordBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Append(raw); err != nil {
			t.Fatal(err)
		}
	}
}

// seededSearchServer builds a web server over a real store holding n records in
// topic "cari" partition 0 (roughly 85 bytes per record).
func seededSearchServer(t *testing.T, n int) *Server {
	t.Helper()
	srv, store, _ := newHealthTestServer(t)
	if _, err := store.CreateTopic("cari", 1); err != nil {
		t.Fatal(err)
	}
	p := store.GetPartition("cari", 0)
	if p == nil {
		t.Fatal("partisi tidak ada")
	}
	appendSearchableMessages(t, p, n)
	return srv
}

func doSearch(t *testing.T, srv *Server, query string) (*httptest.ResponseRecorder, searchResponse) {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/v1/topics/cari/messages?"+query, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var out searchResponse
	if rec.Code == 200 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("respons tidak bisa diurai: %v (body=%s)", err, rec.Body.String())
		}
	}
	return rec, out
}

// TestSearchStopsAtByteBudget is the regression test for the defect this change
// fixes: limit bounds the RESULT, not the work, so a predicate that never
// matches used to walk the whole log.
func TestSearchStopsAtByteBudget(t *testing.T) {
	const total = 20000
	srv := seededSearchServer(t, total)

	rec, out := doSearch(t, srv, "search=tidak-ada-yang-cocok&budget_bytes=4096&limit=50")
	if rec.Code != 200 {
		t.Fatalf("kode %d: %s", rec.Code, rec.Body.String())
	}
	if !out.Truncated {
		t.Fatalf("pemindaian harus ditandai terpotong: %+v", out)
	}
	if out.StopReason != "byte_budget" {
		t.Fatalf("alasan berhenti %q, mau byte_budget", out.StopReason)
	}
	if out.BytesRead < 4096 {
		t.Fatalf("bytesRead=%d, harus mencapai anggaran 4096", out.BytesRead)
	}
	page := int64(50*4096 + 4096) // satu halaman terakhir boleh melewati anggaran
	if out.BytesRead > out.BudgetBytes+page {
		t.Fatalf("bytesRead=%d melewati anggaran %d lebih dari satu halaman (%d)", out.BytesRead, out.BudgetBytes, page)
	}
	if out.Scanned >= total {
		t.Fatalf("scanned=%d: pemindaian tidak berhenti di anggaran, ia menelusuri seluruh log", out.Scanned)
	}
	if out.NextOffset <= 0 {
		t.Fatalf("nextOffset=%d harus menunjuk lanjutan yang belum dipindai", out.NextOffset)
	}
	// Daftar kosong tetap array, bukan null: klien yang menyimpan respons ini
	// memecah daftar berikutnya kalau ia menerima objek.
	if !strings.Contains(rec.Body.String(), `"records":[]`) {
		t.Fatalf("daftar kosong harus [] bukan null: %s", rec.Body.String())
	}
}

// TestSearchTruncationIsResumable proves the truncation is usable: the client
// continues from nextOffset and still reaches the record it was looking for.
func TestSearchTruncationIsResumable(t *testing.T) {
	const total = 20000
	srv := seededSearchServer(t, total)

	offset, found := int64(0), false
	for i := 0; i < 40; i++ {
		rec, out := doSearch(t, srv, fmt.Sprintf("search=msg-19999&budget_bytes=4096&limit=50&offset=%d", offset))
		if rec.Code != 200 {
			t.Fatalf("kode %d: %s", rec.Code, rec.Body.String())
		}
		if out.Count > 0 {
			found = true
			break
		}
		if !out.Truncated {
			t.Fatalf("berhenti tanpa hasil dan tanpa terpotong: %+v", out)
		}
		if out.NextOffset <= offset {
			t.Fatalf("nextOffset tidak maju: %d -> %d", offset, out.NextOffset)
		}
		offset = out.NextOffset
	}
	if !found {
		t.Fatal("target di ujung log tidak ditemukan walau pemindaian dilanjutkan")
	}
}

// TestSearchStopsWhenClientGone covers the second half of the defect: the loop
// had no context check, so a disconnected client left the scan running.
func TestSearchStopsWhenClientGone(t *testing.T) {
	srv := seededSearchServer(t, 20000)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("GET", "/api/v1/topics/cari/messages?search=zzz&limit=50", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var out searchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("respons tidak bisa diurai: %v (body=%s)", err, rec.Body.String())
	}
	if out.Scanned != 0 {
		t.Fatalf("scanned=%d: pemindaian berjalan walau klien sudah pergi", out.Scanned)
	}
	if out.StopReason != "client_gone" {
		t.Fatalf("alasan berhenti %q, mau client_gone", out.StopReason)
	}
	if !out.Truncated {
		t.Fatalf("respons harus jujur menyatakan tidak lengkap: %+v", out)
	}
}

// TestSearchRejectsWhenScansSaturated covers admission control: without it, N
// dashboard tabs mean N concurrent full-log scans.
func TestSearchRejectsWhenScansSaturated(t *testing.T) {
	srv := seededSearchServer(t, 100)

	for i := 0; i < searchMaxConcurrent; i++ {
		searchSem <- struct{}{}
	}
	defer func() {
		for i := 0; i < searchMaxConcurrent; i++ {
			<-searchSem
		}
	}()

	rec, _ := doSearch(t, srv, "limit=10")
	if rec.Code != 503 {
		t.Fatalf("kode %d, mau 503 saat pemindaian sedang penuh (%s)", rec.Code, rec.Body.String())
	}
}

// TestSearchFindsMatchWithinBudget is the positive control: bounding the scan
// must not break an ordinary selective search.
func TestSearchFindsMatchWithinBudget(t *testing.T) {
	srv := seededSearchServer(t, 500)

	rec, out := doSearch(t, srv, "search=msg-00499&limit=10")
	if rec.Code != 200 {
		t.Fatalf("kode %d: %s", rec.Code, rec.Body.String())
	}
	if out.Count != 1 {
		t.Fatalf("count=%d, mau 1: %+v", out.Count, out)
	}
	if out.Truncated {
		t.Fatalf("pencarian yang selesai tidak boleh ditandai terpotong: %+v", out)
	}
	if out.StopReason != "reached_end" {
		t.Fatalf("alasan berhenti %q, mau reached_end", out.StopReason)
	}
}

func TestSearchBudgetClamping(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{"", searchDefaultByteBudget},
		{"abc", searchDefaultByteBudget},
		{"-5", searchDefaultByteBudget},
		{"0", searchDefaultByteBudget},
		{"1024", 1024},
		{"999999999", searchMaxByteBudget},
	}
	for _, c := range cases {
		if got := budgetInt64(c.raw, searchDefaultByteBudget, searchMaxByteBudget); got != c.want {
			t.Fatalf("budgetInt64(%q)=%d, mau %d", c.raw, got, c.want)
		}
	}
}
