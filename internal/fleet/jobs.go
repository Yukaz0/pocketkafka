package fleet

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
)

var (
	ErrJobNotFound          = errors.New("fleet: job not found")
	ErrIllegalJobTransition = errors.New("fleet: illegal job transition")
	ErrJobLimitExceeded     = errors.New("fleet: active job limit reached")
)

// Jobs manages execution lifecycle, idempotency, persistence, and retention of fleet background tasks.
type Jobs struct {
	mu               sync.RWMutex
	filePath         string
	jobs             map[string]*Job
	order            []string
	idempotencyIndex map[string]string
	timeout          time.Duration
	retentionDays    int
	retentionEntries int
	maxActiveJobs    int
}

// NewJobs constructs a Jobs store backed by fleet_jobs.jsonl in dirOrPath.
// If dirOrPath is empty, jobs are kept in-memory only.
func NewJobs(dirOrPath string) *Jobs {
	j := &Jobs{
		jobs:             make(map[string]*Job),
		idempotencyIndex: make(map[string]string),
		retentionDays:    30,
		retentionEntries: 10000,
		maxActiveJobs:    1000,
	}

	if dirOrPath == "" {
		return j
	}

	if fi, err := os.Stat(dirOrPath); err == nil && !fi.IsDir() {
		j.filePath = dirOrPath
	} else if strings.HasSuffix(dirOrPath, ".jsonl") {
		j.filePath = dirOrPath
	} else {
		j.filePath = filepath.Join(dirOrPath, "fleet_jobs.jsonl")
	}

	_ = j.load()
	return j
}

// SetTimeout sets the maximum duration a job may spend in running status before being marked failed.
func (j *Jobs) SetTimeout(d time.Duration) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.timeout = d
}

// SetRetention configures retention policy by age (days) and count (entries).
func (j *Jobs) SetRetention(days int, entries int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if days > 0 {
		j.retentionDays = days
	}
	if entries > 0 {
		j.retentionEntries = entries
	}
}

// SetMaxActiveJobs sets the maximum allowed number of active (queued/running) jobs.
func (j *Jobs) SetMaxActiveJobs(limit int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.maxActiveJobs = limit
}

// Submit enqueues a new job or returns the existing job ID if idempotencyKey has already been submitted.
func (j *Jobs) Submit(serverID, action, target, actor, idempotencyKey string) (string, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if idempotencyKey != "" {
		if existingID, ok := j.idempotencyIndex[idempotencyKey]; ok {
			return existingID, nil
		}
	}

	if j.maxActiveJobs > 0 {
		active := 0
		for _, existing := range j.jobs {
			if existing.Status == JobStatusQueued || existing.Status == JobStatusAccepted || existing.Status == JobStatusRunning {
				active++
			}
		}
		if active >= j.maxActiveJobs {
			return "", fmt.Errorf("%w: maximum %d active jobs allowed", ErrJobLimitExceeded, j.maxActiveJobs)
		}
	}

	id := generateJobID()
	job := &Job{
		ID:             id,
		ServerID:       serverID,
		Action:         action,
		Target:         target,
		Actor:          actor,
		IdempotencyKey: idempotencyKey,
		Status:         JobStatusQueued,
		CreatedAt:      time.Now().UTC(),
	}
	j.jobs[id] = job
	j.order = append(j.order, id)
	if idempotencyKey != "" {
		j.idempotencyIndex[idempotencyKey] = id
	}

	if err := j.appendLocked(job); err != nil {
		delete(j.jobs, id)
		if len(j.order) > 0 && j.order[len(j.order)-1] == id {
			j.order = j.order[:len(j.order)-1]
		}
		if idempotencyKey != "" {
			delete(j.idempotencyIndex, idempotencyKey)
		}
		return "", err
	}

	return id, nil
}

// Start transitions a job from queued to running.
func (j *Jobs) Start(id string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	job, ok := j.jobs[id]
	if !ok {
		return ErrJobNotFound
	}

	if job.Status != JobStatusQueued {
		return fmt.Errorf("%w: cannot start job %s with status %s", ErrIllegalJobTransition, id, job.Status)
	}

	oldStatus := job.Status
	oldStartedAt := job.StartedAt

	job.Status = JobStatusRunning
	job.StartedAt = time.Now().UTC()

	if err := j.appendLocked(job); err != nil {
		job.Status = oldStatus
		job.StartedAt = oldStartedAt
		return err
	}
	return nil
}

// Complete transitions a job from running to succeeded or failed.
func (j *Jobs) Complete(id string, status JobStatus, errMsg ...string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	job, ok := j.jobs[id]
	if !ok {
		return ErrJobNotFound
	}

	if status != JobStatusSucceeded && status != JobStatusFailed {
		return fmt.Errorf("%w: invalid target status %s for Complete", ErrIllegalJobTransition, status)
	}

	if job.Status == JobStatusQueued {
		return fmt.Errorf("%w: cannot complete queued job %s directly, must transition via running", ErrIllegalJobTransition, id)
	}

	if job.Status == JobStatusSucceeded || job.Status == JobStatusFailed {
		return fmt.Errorf("%w: cannot complete job %s already in terminal status %s", ErrIllegalJobTransition, id, job.Status)
	}

	oldStatus := job.Status
	oldCompletedAt := job.CompletedAt
	oldError := job.Error

	job.Status = status
	job.CompletedAt = time.Now().UTC()
	if status == JobStatusFailed && len(errMsg) > 0 {
		job.Error = errMsg[0]
	}

	if err := j.appendLocked(job); err != nil {
		job.Status = oldStatus
		job.CompletedAt = oldCompletedAt
		job.Error = oldError
		return err
	}
	return nil
}

// Fail is a convenience helper for completing a job with failed status.
func (j *Jobs) Fail(id string, errMsg string) error {
	return j.Complete(id, JobStatusFailed, errMsg)
}

// Cancel fails a job that never started (queued): the dispatch target is
// gone (no agent) or unreachable. Complete() rejects queued jobs by design,
// so cancelling needs its own transition with the same rollback guarantee.
func (j *Jobs) Cancel(id string, errMsg string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	job, ok := j.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if job.Status != JobStatusQueued {
		return fmt.Errorf("%w: cannot cancel job %s with status %s (only queued)", ErrIllegalJobTransition, id, job.Status)
	}

	oldStatus := job.Status
	oldError := job.Error

	job.Status = JobStatusFailed
	job.CompletedAt = time.Now().UTC()
	job.Error = errMsg

	if err := j.appendLocked(job); err != nil {
		job.Status = oldStatus
		job.CompletedAt = time.Time{}
		job.Error = oldError
		return err
	}
	return nil
}

// Get returns a copy of the job with the specified ID.
func (j *Jobs) Get(id string) (*Job, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	job, ok := j.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}

	j.checkTimeoutLocked(job)
	return copyJob(job), nil
}

// List returns copies of all jobs in submission order.
func (j *Jobs) List() []*Job {
	j.mu.Lock()
	defer j.mu.Unlock()

	out := make([]*Job, 0, len(j.order))
	for _, id := range j.order {
		if job, ok := j.jobs[id]; ok {
			j.checkTimeoutLocked(job)
			out = append(out, copyJob(job))
		}
	}
	return out
}

// ListByServer returns copies of all jobs dispatched to a specific server.
func (j *Jobs) ListByServer(serverID string) []*Job {
	j.mu.Lock()
	defer j.mu.Unlock()

	var out []*Job
	for _, id := range j.order {
		if job, ok := j.jobs[id]; ok && job.ServerID == serverID {
			j.checkTimeoutLocked(job)
			out = append(out, copyJob(job))
		}
	}
	return out
}

// Prune applies retention policies (age and maximum entries) to terminal jobs.
func (j *Jobs) Prune() (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	cutoff := time.Time{}
	if j.retentionDays > 0 {
		cutoff = time.Now().UTC().Add(-time.Duration(j.retentionDays) * 24 * time.Hour)
	}

	pruned := 0
	newOrder := make([]string, 0, len(j.order))

	for _, id := range j.order {
		job, ok := j.jobs[id]
		if !ok {
			continue
		}

		isTerminal := job.Status == JobStatusSucceeded || job.Status == JobStatusFailed
		shouldPruneAge := isTerminal && !cutoff.IsZero() && job.CreatedAt.Before(cutoff)

		if shouldPruneAge {
			delete(j.jobs, id)
			if job.IdempotencyKey != "" {
				delete(j.idempotencyIndex, job.IdempotencyKey)
			}
			pruned++
			continue
		}

		newOrder = append(newOrder, id)
	}

	if j.retentionEntries > 0 && len(newOrder) > j.retentionEntries {
		excess := len(newOrder) - j.retentionEntries
		kept := make([]string, 0, len(newOrder))
		for _, id := range newOrder {
			job, ok := j.jobs[id]
			if !ok {
				continue
			}
			isTerminal := job.Status == JobStatusSucceeded || job.Status == JobStatusFailed
			if excess > 0 && isTerminal {
				delete(j.jobs, id)
				if job.IdempotencyKey != "" {
					delete(j.idempotencyIndex, job.IdempotencyKey)
				}
				pruned++
				excess--
				continue
			}
			kept = append(kept, id)
		}
		newOrder = kept
	}
	j.order = newOrder

	if j.filePath != "" {
		if err := j.rewriteFileLocked(); err != nil {
			return pruned, err
		}
	}

	return pruned, nil
}

func (j *Jobs) checkTimeoutLocked(job *Job) {
	if job.Status == JobStatusRunning && j.timeout > 0 {
		if time.Since(job.StartedAt) > j.timeout {
			job.Status = JobStatusFailed
			job.CompletedAt = time.Now().UTC()
			job.Error = fmt.Sprintf("job execution timed out after %v", j.timeout)
			if err := j.appendLocked(job); err != nil {
				slog.Warn("fleet: failed to append timed out job", "job_id", job.ID, "err", err)
			}
		}
	}
}

func (j *Jobs) appendLocked(job *Job) error {
	if j.filePath == "" {
		return nil
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("fleet: marshal job %s: %w", job.ID, err)
	}

	dir := filepath.Dir(j.filePath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("fleet: mkdir %s: %w", dir, err)
	}

	f, err := os.OpenFile(j.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("fleet: open %s: %w", j.filePath, err)
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("fleet: write %s: %w", j.filePath, err)
	}

	return nil
}

func (j *Jobs) rewriteFileLocked() error {
	if j.filePath == "" {
		return nil
	}

	var buf strings.Builder
	for _, id := range j.order {
		if job, ok := j.jobs[id]; ok {
			data, err := json.Marshal(job)
			if err != nil {
				return err
			}
			buf.Write(data)
			buf.WriteByte('\n')
		}
	}

	return atomicfile.Write(j.filePath, []byte(buf.String()), 0o600)
}

func (j *Jobs) load() error {
	if j.filePath == "" {
		return nil
	}

	f, err := os.Open(j.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 10<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var job Job
		if err := json.Unmarshal([]byte(line), &job); err != nil {
			continue
		}

		if _, exists := j.jobs[job.ID]; !exists {
			j.order = append(j.order, job.ID)
		}
		j.jobs[job.ID] = &job
		if job.IdempotencyKey != "" {
			j.idempotencyIndex[job.IdempotencyKey] = job.ID
		}
	}

	return scanner.Err()
}

func generateJobID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("job-%d-%s", time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}

func copyJob(j *Job) *Job {
	if j == nil {
		return nil
	}
	c := *j
	return &c
}
