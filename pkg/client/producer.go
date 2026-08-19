package client

import (
	"context"
	"time"

	"github.com/neu/go-kafka-neu/pkg/protocol"
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
// payloads. It supports synchronous (SendSync) and asynchronous (Send) publish.
type Producer struct {
	client *KafkaClient
	cfg    ProducerConfig
}

// NewProducer returns a Producer bound to the given client.
func NewProducer(client *KafkaClient, cfg ProducerConfig) *Producer {
	if cfg.Acks == 0 {
		cfg.Acks = 1
	}
	return &Producer{client: client, cfg: cfg}
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

// Send publishes a message asynchronously. The optional callback fires on
// completion.
func (p *Producer) Send(msg *Message) {
	go func() {
		off, err := p.SendSync(context.Background(), msg)
		if msg.Callback != nil {
			msg.Callback(off, err)
		}
	}()
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
		batch, err := encodeBatch(list)
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

// encodeBatch serializes a list of messages into one RecordBatch.
func encodeBatch(msgs []*Message) ([]byte, error) {
	now := time.Now().UnixMilli()
	b := &protocol.RecordBatch{
		BaseOffset:    0,
		BaseTimestamp: now,
		MaxTimestamp:  now,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
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

// Close releases resources held by the producer.
func (p *Producer) Close() error { return nil }
