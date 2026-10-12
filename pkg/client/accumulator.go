package client

import (
	"context"
	"sync"
	"time"
)

type TopicPartition struct {
	Topic     string
	Partition int32
}

type BatchBuffer struct {
	records   []*Message
	byteSize  int
	createdAt time.Time
}

type RecordAccumulator struct {
	mu        sync.Mutex
	buffers   map[TopicPartition]*BatchBuffer
	batchSize int
	byteLimit int
	linger    time.Duration
	flushFn   func([]*Message) error
	flushCh   chan TopicPartition
	doneCh    chan struct{}
	wg        sync.WaitGroup
}

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
		byteLimit: 16 * 1024,
		linger:    linger,
		flushFn:   flushFn,
		flushCh:   make(chan TopicPartition, 256),
		doneCh:    make(chan struct{}),
	}
	a.wg.Add(1)
	go a.flusherLoop()
	return a
}

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
		}
		return true
	}
	return false
}

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

func (a *RecordAccumulator) Close() {
	select {
	case <-a.doneCh:
	default:
		close(a.doneCh)
	}
	a.wg.Wait()
	a.flushAll()
}

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

func (a *RecordAccumulator) Flush(ctx context.Context) {
	a.flushAll()
}
