package fleet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestJobIllegalTransition(t *testing.T) {
	j := NewJobs(t.TempDir())
	id, err := j.Submit("srv-1", "restart", "c1", "op", "idem-1")
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if err := j.Complete(id, "succeeded"); err == nil {
		t.Fatal("queued→succeeded langsung harus ditolak, wajib via running")
	}
	if err := j.Complete(id, "failed"); err == nil {
		t.Fatal("queued→failed langsung harus ditolak, wajib via running")
	}

	if err := j.Start(id); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if err := j.Start(id); err == nil {
		t.Fatal("running→running harus ditolak")
	}
	if err := j.Complete(id, "succeeded"); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	if err := j.Complete(id, "failed"); err == nil {
		t.Fatal("succeeded→failed harus ditolak")
	}
	if err := j.Start(id); err == nil {
		t.Fatal("succeeded→running harus ditolak")
	}
}

func TestJobIdempotency(t *testing.T) {
	j := NewJobs(t.TempDir())
	a, err := j.Submit("srv-1", "stop", "c1", "op", "idem-9")
	if err != nil {
		t.Fatalf("submit a failed: %v", err)
	}
	b, err := j.Submit("srv-1", "stop", "c1", "op", "idem-9")
	if err != nil {
		t.Fatalf("submit b failed: %v", err)
	}
	if a != b {
		t.Fatal("idempotency key sama harus kembalikan job sama")
	}

	c, err := j.Submit("srv-1", "stop", "c1", "op", "idem-10")
	if err != nil {
		t.Fatalf("submit c failed: %v", err)
	}
	if a == c {
		t.Fatal("idempotency key beda tidak boleh menghasilkan job sama")
	}
}

func TestJobPersistence(t *testing.T) {
	dir := t.TempDir()
	j1 := NewJobs(dir)
	id, err := j1.Submit("srv-1", "restart", "c1", "op", "idem-p1")
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if err := j1.Start(id); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if err := j1.Complete(id, "succeeded"); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	j2 := NewJobs(dir)
	job, err := j2.Get(id)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if job.Status != JobStatusSucceeded {
		t.Fatalf("status = %s, want %s", job.Status, JobStatusSucceeded)
	}
	if job.Action != "restart" || job.ServerID != "srv-1" {
		t.Fatalf("unexpected job data: %+v", job)
	}

	id2, err := j2.Submit("srv-1", "restart", "c1", "op", "idem-p1")
	if err != nil {
		t.Fatalf("submit reload failed: %v", err)
	}
	if id2 != id {
		t.Fatalf("idempotency reload mismatch: %s != %s", id2, id)
	}
}

func TestJobTimeout(t *testing.T) {
	j := NewJobs(t.TempDir())
	j.SetTimeout(50 * time.Millisecond)

	id, err := j.Submit("srv-1", "logs", "c1", "op", "")
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if err := j.Start(id); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(70 * time.Millisecond)

	job, err := j.Get(id)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if job.Status != JobStatusFailed {
		t.Fatalf("job status = %s, want %s due to timeout", job.Status, JobStatusFailed)
	}
	if !strings.Contains(job.Error, "timed out") {
		t.Fatalf("job error = %q, want timeout error", job.Error)
	}
}

func TestJobRetentionPrunesOld(t *testing.T) {
	dir := t.TempDir()
	j := NewJobs(dir)
	j.SetRetention(1, 3)

	var ids []string
	for range 5 {
		id, err := j.Submit("srv-1", "ping", "c1", "op", "")
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
		_ = j.Start(id)
		_ = j.Complete(id, "succeeded")
		ids = append(ids, id)
	}
	pruned, err := j.Prune()
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 2 {
		t.Fatalf("pruned count = %d, want 2", pruned)
	}

	for i := 0; i < 2; i++ {
		if _, err := j.Get(ids[i]); err == nil {
			t.Fatalf("job %s should be pruned", ids[i])
		}
	}

	for i := 2; i < 5; i++ {
		if _, err := j.Get(ids[i]); err != nil {
			t.Fatalf("job %s should exist: %v", ids[i], err)
		}
	}

	raw, err := os.ReadFile(filepath.Join(dir, "fleet_jobs.jsonl"))
	if err != nil {
		t.Fatalf("read fleet_jobs.jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 3 {
		t.Fatalf("file lines count = %d, want 3", len(lines))
	}
}

func TestJobPruneCountPreservesQueuedAndRunning(t *testing.T) {
	dir := t.TempDir()
	j := NewJobs(dir)
	j.SetRetention(0, 2)

	id1, _ := j.Submit("srv-1", "ping", "c1", "op", "idem-term-1")
	_ = j.Start(id1)
	_ = j.Complete(id1, JobStatusSucceeded)

	id2, _ := j.Submit("srv-1", "deploy", "c1", "op", "idem-run-1")
	_ = j.Start(id2)

	id3, _ := j.Submit("srv-1", "upgrade", "c1", "op", "idem-queue-1")

	id4, _ := j.Submit("srv-1", "ping", "c1", "op", "idem-term-2")
	_ = j.Start(id4)
	_ = j.Complete(id4, JobStatusFailed, "disk full")

	pruned, err := j.Prune()
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 2 {
		t.Fatalf("pruned count = %d, want 2 (only terminal jobs)", pruned)
	}

	if _, err := j.Get(id1); err == nil {
		t.Fatalf("terminal job %s should be pruned", id1)
	}
	if _, err := j.Get(id4); err == nil {
		t.Fatalf("terminal job %s should be pruned", id4)
	}

	job2, err := j.Get(id2)
	if err != nil {
		t.Fatalf("running job %s must be preserved: %v", id2, err)
	}
	if job2.Status != JobStatusRunning {
		t.Fatalf("job2 status = %s, want running", job2.Status)
	}

	job3, err := j.Get(id3)
	if err != nil {
		t.Fatalf("queued job %s must be preserved: %v", id3, err)
	}
	if job3.Status != JobStatusQueued {
		t.Fatalf("job3 status = %s, want queued", job3.Status)
	}

	idemID2, err := j.Submit("srv-1", "deploy", "c1", "op", "idem-run-1")
	if err != nil || idemID2 != id2 {
		t.Fatalf("idempotency for living running job lost: got %s, want %s, err: %v", idemID2, id2, err)
	}
	idemID3, err := j.Submit("srv-1", "upgrade", "c1", "op", "idem-queue-1")
	if err != nil || idemID3 != id3 {
		t.Fatalf("idempotency for living queued job lost: got %s, want %s, err: %v", idemID3, id3, err)
	}
}

func TestJobAppendFailureRollback(t *testing.T) {
	dir := t.TempDir()
	roDir := filepath.Join(dir, "readonly_dir")
	if err := os.Mkdir(roDir, 0o500); err != nil {
		t.Fatalf("mkdir roDir: %v", err)
	}
	defer os.Chmod(roDir, 0o700)

	jFail := NewJobs(filepath.Join(roDir, "nested"))
	_, err := jFail.Submit("srv-1", "ping", "c1", "op", "idem-ro-1")
	if err == nil {
		t.Fatal("submit to readonly dir must fail")
	}
	if len(jFail.List()) != 0 {
		t.Fatalf("jobs list after failed submit should be empty, got %d", len(jFail.List()))
	}
	if len(jFail.idempotencyIndex) != 0 {
		t.Fatalf("idempotencyIndex after failed submit should be empty, got %d", len(jFail.idempotencyIndex))
	}

	filePath := filepath.Join(dir, "fleet_jobs.jsonl")
	j := NewJobs(dir)
	id, err := j.Submit("srv-1", "ping", "c1", "op", "idem-1")
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	if err := os.Chmod(filePath, 0o400); err != nil {
		t.Fatalf("chmod 0400: %v", err)
	}
	defer os.Chmod(filePath, 0o600)

	if err := j.Start(id); err == nil {
		t.Fatal("start on read-only file must fail append")
	}
	job, err := j.Get(id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != JobStatusQueued {
		t.Fatalf("job status after failed Start = %s, want queued (rolled back)", job.Status)
	}
	if !job.StartedAt.IsZero() {
		t.Fatalf("job StartedAt after failed Start should be zero, got %v", job.StartedAt)
	}

	if err := os.Chmod(filePath, 0o600); err != nil {
		t.Fatalf("chmod 0600: %v", err)
	}
	if err := j.Start(id); err != nil {
		t.Fatalf("start after permission restore failed: %v", err)
	}

	if err := os.Chmod(filePath, 0o400); err != nil {
		t.Fatalf("chmod 0400: %v", err)
	}
	if err := j.Complete(id, JobStatusSucceeded); err == nil {
		t.Fatal("complete on read-only file must fail append")
	}
	job, err = j.Get(id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != JobStatusRunning {
		t.Fatalf("job status after failed Complete = %s, want running (rolled back)", job.Status)
	}
	if !job.CompletedAt.IsZero() {
		t.Fatalf("job CompletedAt after failed Complete should be zero, got %v", job.CompletedAt)
	}
}

func TestJobsLargeLineLoad(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "fleet_jobs.jsonl")
	largeTarget := strings.Repeat("A", 128*1024)

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	line := fmt.Sprintf(`{"id":"job-large-1","server_id":"srv-1","action":"test","target":"%s","actor":"op","status":"queued","created_at":"2026-10-26T00:00:00Z"}`+"\n", largeTarget)
	if _, err := f.WriteString(line); err != nil {
		t.Fatalf("write line: %v", err)
	}
	f.Close()

	j := NewJobs(dir)
	job, err := j.Get("job-large-1")
	if err != nil {
		t.Fatalf("failed to load job with large line >64KB: %v", err)
	}
	if len(job.Target) != 128*1024 {
		t.Fatalf("loaded target length = %d, want %d", len(job.Target), 128*1024)
	}
}

func TestCancelQueuedJob(t *testing.T) {
	j := NewJobs(t.TempDir())
	id, _ := j.Submit("s1", "restart", "c1", "op", "idem-cancel-1")
	if err := j.Cancel(id, "no agent"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	got, _ := j.Get(id)
	if got.Status != JobStatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.Error != "no agent" {
		t.Fatalf("error = %q", got.Error)
	}
	if err := j.Cancel(id, "again"); err == nil {
		t.Fatal("second Cancel must fail (terminal)")
	}
}
