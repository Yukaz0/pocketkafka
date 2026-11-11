package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func openTestPartition(t *testing.T, maxSegmentBytes int64) *Partition {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "t", "0")
	p, err := OpenPartition(dir, "t", 0, maxSegmentBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func appendKV(t *testing.T, p *Partition, kvs ...[2]string) {
	t.Helper()
	pairs := make([][2][]byte, 0, len(kvs))
	for _, kv := range kvs {
		pairs = append(pairs, [2][]byte{[]byte(kv[0]), []byte(kv[1])})
	}
	if _, err := p.Append(makeKeyedBatch(pairs)); err != nil {
		t.Fatal(err)
	}
}

func values(t *testing.T, p *Partition) []string {
	t.Helper()
	out := []string{}
	for _, kv := range readAllValues(t, p) {
		out = append(out, kv[0]+"="+kv[1])
	}
	return out
}

func valuesPaged(t *testing.T, p *Partition) []string {
	t.Helper()
	out := []string{}
	for next := int64(0); next < p.LogEndOffset(); {
		raw, _, err := p.Read(next, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 {
			break
		}
		pos, last := 0, int64(-1)
		for pos+12 <= len(raw) {
			h, err := ParseRecordBatchHeader(raw[pos:])
			if err != nil {
				break
			}
			total := int(BatchTotalSize(h))
			if total <= 0 || pos+total > len(raw) {
				break
			}
			if b, err := protocol.DecodeRecordBatch(raw[pos : pos+total]); err == nil {
				base := int64(binary.BigEndian.Uint64(raw[pos : pos+8]))
				for _, r := range b.Records {
					out = append(out, string(r.Key)+"="+string(r.Value))
					last = base + int64(r.OffsetDelta)
				}
			}
			pos += total
		}
		if last < next {
			break
		}
		next = last + 1
	}
	return out
}

func TestCompactHeapStaysBelowPartitionSize(t *testing.T) {
	const (
		records   = 1500
		valueSize = 16 << 10
	)
	p := openTestPartition(t, 8<<20)

	payload := make([]byte, valueSize)
	for i := range payload {
		payload[i] = 'x'
	}
	for i := 0; i < records; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		if _, err := p.Append(makeKeyedBatch([][2][]byte{{k, payload}})); err != nil {
			t.Fatal(err)
		}
	}
	dataBytes := int64(records * valueSize)

	var baseline runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&baseline)

	var peak uint64
	stop := make(chan struct{})
	var sampling atomic.Bool
	sampling.Store(true)
	go func() {
		var ms runtime.MemStats
		for sampling.Load() {
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > atomic.LoadUint64(&peak) {
				atomic.StoreUint64(&peak, ms.HeapAlloc)
			}
			time.Sleep(2 * time.Millisecond)
		}
		close(stop)
	}()

	if err := p.Compact(); err != nil {
		sampling.Store(false)
		<-stop
		t.Fatalf("compact: %v", err)
	}
	sampling.Store(false)
	<-stop

	growth := int64(atomic.LoadUint64(&peak)) - int64(baseline.HeapAlloc)
	if growth < 0 {
		growth = 0
	}
	if growth > dataBytes/2 {
		t.Fatalf("puncak heap naik %d byte untuk partisi %d byte: pemindaian tidak streaming", growth, dataBytes)
	}
	got := valuesPaged(t, p)
	if len(got) != records {
		t.Fatalf("hasil kompaksi salah: %d record, mau %d", len(got), records)
	}
	if got[0] != "k00000="+string(payload) {
		t.Fatalf("record pertama salah setelah kompaksi")
	}
}

func TestCompactWithoutDuplicatesRewritesNothing(t *testing.T) {
	p := openTestPartition(t, 1<<20)
	for i := 0; i < 50; i++ {
		appendKV(t, p, [2]string{fmt.Sprintf("k%02d", i), "v"})
	}
	before, err := os.ReadDir(p.dir)
	if err != nil {
		t.Fatal(err)
	}
	stamps := map[string]time.Time{}
	for _, e := range before {
		stamps[e.Name()] = statMtime(t, filepath.Join(p.dir, e.Name()))
	}

	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}

	after, err := os.ReadDir(p.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("berkas berubah jumlahnya: %d -> %d", len(before), len(after))
	}
	for _, e := range after {
		if got := statMtime(t, filepath.Join(p.dir, e.Name())); !got.Equal(stamps[e.Name()]) {
			t.Fatalf("berkas %s ditulis ulang padahal tidak ada key yang digantikan", e.Name())
		}
	}
}

func statMtime(t *testing.T, path string) time.Time {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.ModTime()
}

func TestCompactRefusesConcurrentRun(t *testing.T) {
	p := openTestPartition(t, 1<<20)
	appendKV(t, p, [2]string{"a", "1"}, [2]string{"a", "2"})

	p.compacting.Store(true)
	err := p.Compact()
	p.compacting.Store(false)
	if !errors.Is(err, ErrCompactionRunning) {
		t.Fatalf("err=%v, mau ErrCompactionRunning", err)
	}
	if got := values(t, p); len(got) != 2 {
		t.Fatalf("data tersentuh saat kompaksi ditolak: %v", got)
	}
}

func TestCompactCarriesRecordsAppendedDuringPlan(t *testing.T) {
	p := openTestPartition(t, 1<<20)
	appendKV(t, p, [2]string{"a", "1"})
	appendKV(t, p, [2]string{"a", "2"})
	appendKV(t, p, [2]string{"b", "3"})

	p.mu.RLock()
	segs, end := p.compactionSource()
	plan, err := planCompaction(segs, end)
	if err != nil {
		p.mu.RUnlock()
		t.Fatal(err)
	}
	w, err := stageCompaction(filepath.Join(p.dir, compactTmpDir), p.maxSegmentBytes, p.indexInterval, segs, end, plan)
	p.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	appendKV(t, p, [2]string{"c", "4"})

	p.mu.Lock()
	err = p.installCompaction(w, end)
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	got := values(t, p)
	want := []string{"a=2", "b=3", "c=4"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d: got %s want %s", i, got[i], want[i])
		}
	}
}

func TestCompactionSwapRecoversFromCrash(t *testing.T) {
	states := []struct {
		name    string
		prepare func(t *testing.T, dir string, m compactionManifest)
	}{
		{"new set staged, nothing swapped", func(t *testing.T, dir string, m compactionManifest) {}},
		{"old files half deleted", func(t *testing.T, dir string, m compactionManifest) {
			os.Remove(filepath.Join(dir, m.Old[0]))
		}},
		{"new files half installed", func(t *testing.T, dir string, m compactionManifest) {
			os.Remove(filepath.Join(dir, m.Old[0]))
			tmp := filepath.Join(dir, compactTmpDir)
			if err := os.Rename(filepath.Join(tmp, m.New[0]+compactStaged), filepath.Join(dir, m.New[0])); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, st := range states {
		t.Run(st.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "t", "0")
			p, err := OpenPartition(dir, "t", 0, 1<<20, 1)
			if err != nil {
				t.Fatal(err)
			}
			appendKV(t, p, [2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"a", "3"})

			p.mu.RLock()
			segs, end := p.compactionSource()
			plan, err := planCompaction(segs, end)
			if err != nil {
				p.mu.RUnlock()
				t.Fatal(err)
			}
			w, err := stageCompaction(filepath.Join(dir, compactTmpDir), p.maxSegmentBytes, p.indexInterval, segs, end, plan)
			if err != nil {
				p.mu.RUnlock()
				t.Fatal(err)
			}
			p.mu.RUnlock()
			names, err := w.finish()
			if err != nil {
				t.Fatal(err)
			}
			tmp := filepath.Join(dir, compactTmpDir)
			if err := stageSwapFiles(tmp, names); err != nil {
				t.Fatal(err)
			}
			old, err := dirFileNames(dir)
			if err != nil {
				t.Fatal(err)
			}
			m := compactionManifest{Old: old, New: names}
			if err := writeManifest(tmp, m); err != nil {
				t.Fatal(err)
			}
			st.prepare(t, dir, m)
			p.Close()

			reopened, err := OpenPartition(dir, "t", 0, 1<<20, 1)
			if err != nil {
				t.Fatalf("open setelah crash: %v", err)
			}
			defer reopened.Close()

			got := values(t, reopened)
			want := []string{"b=2", "a=3"}
			if len(got) != len(want) {
				t.Fatalf("got %v want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("record %d: got %s want %s", i, got[i], want[i])
				}
			}
			if _, err := os.Stat(filepath.Join(dir, compactTmpDir)); !os.IsNotExist(err) {
				t.Fatalf("direktori staging tertinggal setelah pemulihan")
			}
		})
	}
}

func TestCompactionStagingDiscardedWithoutManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "t", "0")
	p, err := OpenPartition(dir, "t", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	appendKV(t, p, [2]string{"a", "1"}, [2]string{"b", "2"})
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	p.Close()

	tmp := filepath.Join(dir, compactTmpDir)
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "00000000000000000000.log"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPartition(dir, "t", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := values(t, reopened); len(got) != 2 {
		t.Fatalf("data lama hilang: %v", got)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("staging yang tidak lengkap tidak dibuang")
	}
}

func TestCompactRollsSegments(t *testing.T) {
	const (
		maxSeg  = 32 << 10
		records = 4000
	)
	p := openTestPartition(t, maxSeg)
	payload := make([]byte, 20)
	for i := 0; i < records; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		if _, err := p.Append(makeKeyedBatch([][2][]byte{{k, payload}})); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	infos := p.SegmentInfos()
	if len(infos) < 3 {
		t.Fatalf("kompaksi menulis %d segmen; roll tidak terjadi", len(infos))
	}
	slack := int64(compactBatchSize*40 + 4096)
	for _, si := range infos {
		if si.SizeBytes > maxSeg+slack {
			t.Fatalf("segmen %d berukuran %d byte, jauh di atas batas %d", si.BaseOffset, si.SizeBytes, maxSeg)
		}
	}
}
