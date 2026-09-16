package client

import (
	"context"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// Message is a single record to publish.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Partition int32 // -1 for auto hash (partition 0 in this single-partition broker)
	Callback  func(offset int64, err error)
}

// Producer publishes messages to the broker, batching them into RecordBatch v2
// payloads. It supports synchronous (SendSync), buffered asynchronous (Send),
// idempotent (sequence-numbered) and transactional publish.
type Producer struct {
	client *KafkaClient
	cfg    ProducerConfig

	accum *RecordAccumulator

	// Idempotent producer state. Transactional state is deliberately absent:
	// the broker does not implement transactions (see ErrTransactionsUnsupported).
	producerID    int64
	producerEpoch int16
	sequence      int32 // per-partition base sequence (single-partition broker)
	mu            sync.Mutex
}

// NewProducer returns a Producer bound to the given client. When the config
// enables idempotence or a transactional ID, the producer initializes itself
// with the broker (InitProducerId, Key 22).
func NewProducer(client *KafkaClient, cfg ProducerConfig) *Producer {
	if cfg.Acks == 0 {
		cfg.Acks = 1
	}
	p := &Producer{
		client:        client,
		cfg:           cfg,
		producerID:    -1,
		producerEpoch: -1,
		sequence:      0,
	}
	if cfg.Idempotent || cfg.TransactionalID != "" {
		p.initProducerID()
	}
	// Buffered producer: a RecordAccumulator flushes on batch-size/linger.
	if cfg.BatchSize > 1 || cfg.LingerMs > 0 {
		p.accum = NewRecordAccumulator(cfg.BatchSize, cfg.LingerMs, p.flushBatch)
	}
	return p
}

// initProducerID requests a producer ID from the broker (Key 22).
func (p *Producer) initProducerID() {
	req := &protocol.InitProducerIdRequest{
		Version:              p.client.version(protocol.APKInitProducerID),
		TransactionTimeoutMs: int32(p.cfg.Timeout / time.Millisecond),
	}
	if p.cfg.TransactionalID != "" {
		req.TransactionalID = &p.cfg.TransactionalID
	}
	body, err := protocol.EncodeInitProducerIdRequest(req)
	if err != nil {
		return
	}
	respBody, err := p.client.roundTrip(protocol.APKInitProducerID, req.Version, body)
	if err != nil {
		return
	}
	resp, err := protocol.DecodeInitProducerIdResponse(req.Version, respBody)
	if err != nil || resp.ErrorCode != protocol.ErrNone {
		return
	}
	p.producerID = resp.ProducerID
	p.producerEpoch = resp.ProducerEpoch
}

// SendSync publishes a single message and waits for the broker ack, returning
// the assigned base offset.
func (p *Producer) SendSync(ctx context.Context, msg *Message) (int64, error) {
	req, err := p.buildProduceRequest([]*Message{msg})
	if err != nil {
		return -1, err
	}
	body, err := protocol.EncodeProduceRequest(req)
	if err != nil {
		return -1, err
	}
	respBody, err := p.client.roundTrip(protocol.APKProduce, req.Version, body)
	if err != nil {
		return -1, err
	}
	resp, err := protocol.DecodeProduceResponse(req.Version, respBody)
	if err != nil {
		return -1, err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return -1, errNoResponse
	}
	pr := resp.Topics[0].Partitions[0]
	if pr.ErrorCode != protocol.ErrNone {
		return -1, &KafkaError{Code: pr.ErrorCode}
	}
	return pr.BaseOffset, nil
}

// Send publishes a message asynchronously. When the producer is buffered
// (BatchSize/LingerMs configured) the message is queued and flushed by the
// accumulator; otherwise it is published immediately in a goroutine. The
// optional callback fires on completion.
func (p *Producer) Send(msg *Message) {
	if p.accum != nil {
		p.accum.Append(msg)
		return
	}
	go func() {
		off, err := p.SendSync(context.Background(), msg)
		if msg.Callback != nil {
			msg.Callback(off, err)
		}
	}()
}

// Flush blocks until all buffered messages are sent and acknowledged.
func (p *Producer) Flush(ctx context.Context) error {
	if p.accum != nil {
		p.accum.Flush(ctx)
	}
	return nil
}

// flushBatch is the accumulator flush callback: one ProduceRequest per batch.
func (p *Producer) flushBatch(msgs []*Message) error {
	req, err := p.buildProduceRequest(msgs)
	if err != nil {
		return err
	}
	body, err := protocol.EncodeProduceRequest(req)
	if err != nil {
		return err
	}
	_, err = p.client.roundTrip(protocol.APKProduce, req.Version, body)
	return err
}

// buildProduceRequest groups messages by topic and encodes them into a single
// RecordBatch per topic (partition 0).
func (p *Producer) buildProduceRequest(msgs []*Message) (*protocol.ProduceRequest, error) {
	byTopic := make(map[string][]*Message)
	for _, m := range msgs {
		byTopic[m.Topic] = append(byTopic[m.Topic], m)
	}

	req := &protocol.ProduceRequest{
		Version: p.client.version(protocol.APKProduce),
		Acks:    p.cfg.Acks,
		Timeout: int32(p.cfg.Timeout / time.Millisecond),
	}
	for topic, list := range byTopic {
		batch, err := p.encodeBatch(list)
		if err != nil {
			return nil, err
		}
		req.Topics = append(req.Topics, protocol.ProduceRequestTopic{
			Topic: topic,
			Partitions: []protocol.ProduceRequestPartition{{
				Partition: 0,
				Records:   batch,
			}},
		})
	}
	return req, nil
}

// encodeBatch serializes a list of messages into one RecordBatch, stamping the
// producer ID/epoch/sequence when the producer is idempotent or transactional.
func (p *Producer) encodeBatch(msgs []*Message) ([]byte, error) {
	now := time.Now().UnixMilli()
	b := &protocol.RecordBatch{
		BaseOffset:    0,
		BaseTimestamp: now,
		MaxTimestamp:  now,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
	}
	if p.cfg.Idempotent || p.cfg.TransactionalID != "" {
		p.mu.Lock()
		b.ProducerID = p.producerID
		b.ProducerEpoch = p.producerEpoch
		b.BaseSequence = p.sequence
		p.sequence += int32(len(msgs))
		p.mu.Unlock()
	}
	for i, m := range msgs {
		var headers []protocol.RecordHeader
		for k, v := range m.Headers {
			headers = append(headers, protocol.RecordHeader{Key: k, Value: v})
		}
		b.Records = append(b.Records, protocol.Record{
			OffsetDelta: int32(i),
			Key:         m.Key,
			Value:       m.Value,
			Headers:     headers,
		})
	}
	return protocol.EncodeRecordBatch(b)
}

// BeginTransaction always fails with ErrTransactionsUnsupported. The broker
// does not implement transactional semantics, so the producer must not pretend
// a transaction started.
func (p *Producer) BeginTransaction() error {
	if p.cfg.TransactionalID == "" {
		return errNoTransaction
	}
	return ErrTransactionsUnsupported
}

// CommitTransaction always fails with ErrTransactionsUnsupported (see
// BeginTransaction).
func (p *Producer) CommitTransaction() error {
	return p.endTransaction(true)
}

// AbortTransaction always fails with ErrTransactionsUnsupported (see
// BeginTransaction).
func (p *Producer) AbortTransaction() error {
	return p.endTransaction(false)
}

func (p *Producer) endTransaction(commit bool) error {
	_ = commit
	if p.cfg.TransactionalID == "" {
		return errNoTransaction
	}
	return ErrTransactionsUnsupported
}

// Close flushes pending batches and releases resources held by the producer.
func (p *Producer) Close() error {
	if p.accum != nil {
		p.accum.Close()
	}
	return nil
}

// errNoTransaction is returned when a transactional API is used without a
// transactional producer.
var errNoTransaction = &KafkaError{Code: protocol.ErrInvalidTxnState}
