package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func benchRaw(tb testing.TB, value string) []byte {
	tb.Helper()
	now := time.Now().UnixMilli()
	raw, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Key: []byte("k"), Value: []byte(value)}},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return raw
}

// BenchmarkPartitionAppend measures the append path (no fsync).
func BenchmarkPartitionAppend(b *testing.B) {
	p, err := OpenPartition(filepath.Join(b.TempDir(), "0"), "t", 0, 1<<30, 4096)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	raw := benchRaw(b, "value")
	// The batch's base offset is rewritten on each append, so re-encode per
	// iteration from a pristine copy.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := append([]byte(nil), raw...)
		if _, err := p.Append(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPartitionAppendSynced measures the durable append path (fsync per
// request, i.e. flush_policy=request).
func BenchmarkPartitionAppendSynced(b *testing.B) {
	p, err := OpenPartition(filepath.Join(b.TempDir(), "0"), "t", 0, 1<<30, 4096)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	raw := benchRaw(b, "value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := append([]byte(nil), raw...)
		if _, err := p.AppendSynced(batch, true); err != nil {
			b.Fatal(err)
		}
	}
}
