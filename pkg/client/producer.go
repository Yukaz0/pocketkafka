package client

import (
	"context"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Partition int32
	Callback  func(offset int64, err error)
}

type Producer struct {
	client *KafkaClient
	cfg    ProducerConfig

	accum *RecordAccumulator

	producerID    int64
	producerEpoch int16
	sequence      int32
	mu            sync.Mutex
}

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
	if cfg.BatchSize > 1 || cfg.LingerMs > 0 {
		p.accum = NewRecordAccumulator(cfg.BatchSize, cfg.LingerMs, p.flushBatch)
	}
	return p
}

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

func (p *Producer) Flush(ctx context.Context) error {
	if p.accum != nil {
		p.accum.Flush(ctx)
	}
	return nil
}

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

func (p *Producer) BeginTransaction() error {
	if p.cfg.TransactionalID == "" {
		return errNoTransaction
	}
	return ErrTransactionsUnsupported
}

func (p *Producer) CommitTransaction() error {
	return p.endTransaction(true)
}

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

func (p *Producer) Close() error {
	if p.accum != nil {
		p.accum.Close()
	}
	return nil
}

var errNoTransaction = &KafkaError{Code: protocol.ErrInvalidTxnState}
