package client

import "errors"

// KafkaError is an error carrying a Kafka protocol error code.
type KafkaError struct {
	Code int16
}

func (e *KafkaError) Error() string {
	return "kafka error code " + itoa(int64(e.Code))
}

var errNoResponse = errors.New("empty produce response")

// ErrTransactionsUnsupported is returned by the transactional producer API. The
// broker does not implement transaction semantics (no transaction coordinator,
// no atomic visibility), so starting, committing, or aborting a transaction
// fails closed with a clear sentinel instead of silently succeeding.
var ErrTransactionsUnsupported = errors.New("pocketkafka: transactions are not supported by this broker")

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
