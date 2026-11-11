package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func FuzzReadRequestFrame(f *testing.F) {

	good := make([]byte, 0, 32)
	body := []byte{0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00}
	frame := make([]byte, 4)
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	good = append(good, frame...)
	good = append(good, body...)
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0, 0, 0, 5, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		const maxSize = 1 << 20
		hdr, _, err := ReadRequestFrame(bytes.NewReader(data), maxSize)
		if err == nil && hdr == nil {
			t.Fatal("nil header with nil error")
		}
	})
}

func FuzzDecodeRequestHeader(f *testing.F) {
	f.Add([]byte{0x00, 0x03, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07, 'c', 'l', 'i', 'e', 'n', 't', '1'})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeRequestHeader(NewReader(data))
	})
}
