package storage

import (
	"path/filepath"
	"testing"

	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// makeKeyedBatch builds a record batch with the given key/value pairs.
func makeKeyedBatch(kvs [][2][]byte) []byte {
	b := &protocol.RecordBatch{
		BaseTimestamp: 1700000000000,
		MaxTimestamp:  1700000000000,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
	}
	for i, kv := range kvs {
		b.Records = append(b.Records, protocol.Record{OffsetDelta: int32(i), Key: kv[0], Value: kv[1]})
	}
	raw, _ := protocol.EncodeRecordBatch(b)
	return raw
}

func readAllValues(t *testing.T, p *Partition) [][2]string {
	t.Helper()
	raw, _, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var out [][2]string
	pos := 0
	for pos < len(raw) {
		h, err := ParseRecordBatchHeader(raw[pos:])
		if err != nil {
			break
		}
		total := int(BatchTotalSize(h))
		b, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
		if err == nil {
			for _, r := range b.Records {
				out = append(out, [2]string{string(r.Key), string(r.Value)})
			}
		}
		pos += total
	}
	return out
}

func TestLogCompaction(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(filepath.Join(dir, "t", "0"), "t", 0, 1024*1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Append: a1, b2, a3, c4, b5, (null)6
	p.Append(makeKeyedBatch([][2][]byte{{[]byte("a"), []byte("1")}}))
	p.Append(makeKeyedBatch([][2][]byte{{[]byte("b"), []byte("2")}}))
	p.Append(makeKeyedBatch([][2][]byte{{[]byte("a"), []byte("3")}}))
	p.Append(makeKeyedBatch([][2][]byte{{[]byte("c"), []byte("4")}}))
	p.Append(makeKeyedBatch([][2][]byte{{[]byte("b"), []byte("5")}}))
	p.Append(makeKeyedBatch([][2][]byte{{nil, []byte("6")}}))

	if p.LogEndOffset() != 6 {
		t.Fatalf("LEO before compact: got %d want 6", p.LogEndOffset())
	}

	if err := p.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Expect the last occurrence of each key in log order: a->3, c->4, b->5,
	// null->6 (4 records, contiguous offsets).
	got := readAllValues(t, p)
	want := [][2]string{{"a", "3"}, {"c", "4"}, {"b", "5"}, {"", "6"}}
	if len(got) != len(want) {
		t.Fatalf("after compact got %d records: %v", len(got), got)
	}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("record %d: got %v want %v", i, got[i], want[i])
		}
	}
	if p.LogEndOffset() != 4 {
		t.Fatalf("LEO after compact: got %d want 4", p.LogEndOffset())
	}

	// Data must survive a reopen.
	p.Close()
	p2, err := OpenPartition(filepath.Join(dir, "t", "0"), "t", 0, 1024*1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	got2 := readAllValues(t, p2)
	if len(got2) != len(want) {
		t.Fatalf("after reopen got %d records want %d", len(got2), len(want))
	}
}

func TestValidateTopicName(t *testing.T) {
	valid := []string{"orders", "user_events", "topic-1", "a.b.c"}
	for _, name := range valid {
		if err := ValidateTopicName(name); err != nil {
			t.Fatalf("expected valid %q: %v", name, err)
		}
	}
	invalid := []string{"", ".", "..", "a/b", "a\\b", "a b", "a:b"}
	for _, name := range invalid {
		if err := ValidateTopicName(name); err == nil {
			t.Fatalf("expected invalid %q", name)
		}
	}
}
