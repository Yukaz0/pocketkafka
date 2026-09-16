// Package protocol is a hand-written, zero external dependency implementation
// of the Apache Kafka binary wire protocol. It provides BigEndian binary
// readers/writers, frame slicing, a RecordBatch v2 codec with Castagnoli
// CRC-32C, and request/response codecs for the API keys the broker supports.
//
// The client-side codecs are grouped by API in client_*.go files; this file
// holds only helpers shared between them.
package protocol

func readInt64Array(r *Reader) ([]int64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
