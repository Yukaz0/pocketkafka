package client

import "errors"

type KafkaError struct {
	Code int16
}

func (e *KafkaError) Error() string {
	return "kafka error code " + itoa(int64(e.Code))
}

var errNoResponse = errors.New("empty produce response")

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
