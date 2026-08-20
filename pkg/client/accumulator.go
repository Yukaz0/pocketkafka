package client

import (
	"context"
	"sync"
	"time"
)

// TopicPartition identifies one partition for batching.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// BatchBuffer accumulates messages for one topic-partition until the batch size
// or linger timeout triggers a flush.
type BatchBuffer struct {
	records   []*Message
	byteSize  int
	createdAt time.Time
}

// RecordAccumulator buffers messages per topic-partition and flushes batches
// either when the batch fills or after the linger window elapses.
type RecordAccumulator struct {
	mu        sync.Mutex
	buffers   map[TopicPartition]*BatchBuffer
	batchSize int           // max records per batch
	byteLimit int           // max payload bytes per batch
	linger    time.Duration // flush timer per batch
	flushFn   func([]*Message) error
	flushCh   chan TopicPartition
	doneCh    chan struct{}
	wg        sync.WaitGroup
}

// NewRecordAccumulator builds an accumulator that flushes full or lingered
// batches through flushFn (which must perform one ProduceRequest).
func NewRecordAccumulator(batchSize int, linger time.Duration, flushFn func([]*Message) error) *RecordAccumulator {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if linger <= 0 {
		linger = 5 * time.Millisecond
	}
	a := &RecordAccumulator{
		buffers:   make(map[TopicPartition]*BatchBuffer),
		batchSize: batchSize,
		byteLimit: 16 * 1024, // 16KB default batch byte limit
		linger:    linger,
		flushFn:   flushFn,
		flushCh:   make(chan TopicPartition, 256),
		doneCh:    make(chan struct{}),
	}
	a.wg.Add(1)
	go a.flusherLoop()
	return a
}

// Append buffers a message and returns true when the batch is full and should
// be flushed immediately.
func (a *RecordAccumulator) Append(msg *Message) bool {
	a.mu.Lock()
	tp := TopicPartition{Topic: msg.Topic, Partition: msg.Partition}
	buf, ok := a.buffers[tp]
	if !ok {
		buf = &BatchBuffer{createdAt: time.Now()}
		a.buffers[tp] = buf
	}
	buf.records = append(buf.records, msg)
	buf.byteSize += len(msg.Key) + len(msg.Value)
	full := len(buf.records) >= a.batchSize || buf.byteSize >= a.byteLimit
	a.mu.Unlock()

	if full {
		select {
		case a.flushCh <- tp:
		default:
			// flusher is busy; it will pick this batch up on its next tick
		}
		return true
	}
	return false
}

// flusherLoop periodically flushes batches whose linger window elapsed.
func (a *RecordAccumulator) flusherLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.linger / 2)
	defer ticker.Stop()
	for {
		select {
		case <-a.doneCh:
			return
		case tp := <-a.flushCh:
			a.flush(tp)
		case <-ticker.C:
			a.flushLingered()
		}
	}
}

func (a *RecordAccumulator) flushLingered() {
	a.mu.Lock()
	var ready []TopicPartition
	now := time.Now()
	for tp, buf := range a.buffers {
		if now.Sub(buf.createdAt) >= a.linger {
			ready = append(ready, tp)
		}
	}
	a.mu.Unlock()
	for _, tp := range ready {
		a.flush(tp)
	}
}

// flush extracts and sends the batch for one topic-partition.
func (a *RecordAccumulator) flush(tp TopicPartition) {
	a.mu.Lock()
	buf, ok := a.buffers[tp]
	if !ok || len(buf.records) == 0 {
		a.mu.Unlock()
		return
	}
	msgs := buf.records
	delete(a.buffers, tp)
	a.mu.Unlock()

	if a.flushFn != nil {
		_ = a.flushFn(msgs)
	}
}

// Close flushes any lingering batches and stops the flusher.
func (a *RecordAccumulator) Close() {
	select {
	case <-a.doneCh:
	default:
		close(a.doneCh)
	}
	a.wg.Wait()
	a.flushAll()
}

// flushAll drains every pending buffer synchronously.
func (a *RecordAccumulator) flushAll() {
	a.mu.Lock()
	var tps []TopicPartition
	for tp := range a.buffers {
		tps = append(tps, tp)
	}
	a.mu.Unlock()
	for _, tp := range tps {
		a.flush(tp)
	}
}

// Flush synchronously drains all pending batches (used by producer.Flush).
func (a *RecordAccumulator) Flush(ctx context.Context) {
	a.flushAll()
}
