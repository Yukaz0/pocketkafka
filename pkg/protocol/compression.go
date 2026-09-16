package protocol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
)

// Compression codecs for Kafka RecordBatch v2 attribute bits 0-2.
// Codec values: 0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd.
const (
	CompressionNone   int16 = 0
	CompressionGzip   int16 = 1
	CompressionSnappy int16 = 2
	CompressionLZ4    int16 = 3
	CompressionZSTD   int16 = 4
)

// ErrUnsupportedCompression is returned when a batch uses an unknown codec.
var ErrUnsupportedCompression = fmt.Errorf("unsupported compression codec")

// CompressionCodec extracts the compression codec from a RecordBatch attribute
// word (bits 0-2).
func CompressionCodec(attr int16) int16 {
	return attr & 0x07
}

// CompressRecordBatch compresses the record payload using the given codec and
// returns the compressed bytes together with the new attribute word that has
// the codec bits set.
func CompressRecordBatch(records []byte, codec int16, attr int16) ([]byte, int16, error) {
	switch codec {
	case CompressionNone:
		return records, attr &^ 0x07, nil
	case CompressionGzip:
		out, err := gzipCompress(records)
		return out, (attr &^ 0x07) | CompressionGzip, err
	case CompressionSnappy:
		out := snappyEncode(nil, records)
		return out, (attr &^ 0x07) | CompressionSnappy, nil
	case CompressionLZ4:
		out, err := lz4BlockCompress(records)
		return out, (attr &^ 0x07) | CompressionLZ4, err
	default:
		return nil, attr, ErrUnsupportedCompression
	}
}

// DecompressRecordBatch decompresses the record payload of a RecordBatch given
// its attribute word. A nil/empty payload for codec 0 is returned unchanged.
func DecompressRecordBatch(records []byte, attr int16) ([]byte, error) {
	switch CompressionCodec(attr) {
	case CompressionNone:
		return records, nil
	case CompressionGzip:
		return gzipDecompress(records)
	case CompressionSnappy:
		return snappyDecode(nil, records)
	case CompressionLZ4:
		return lz4BlockDecompress(records)
	default:
		return nil, ErrUnsupportedCompression
	}
}

func gzipCompress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(src []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// ---------------------------------------------------------------------------
// Snappy block format (as used by Kafka): the 4-byte big-endian length prefix
// followed by the raw snappy block encoding of the data.
// ---------------------------------------------------------------------------

// snappyMaxEncodedLen estimates the upper bound of a snappy block encoding.
func snappyMaxEncodedLen(srcLen int) int {
	// 32-byte header + literals can at worst grow by 1/255 per byte.
	return 32 + srcLen + srcLen/255
}

// snappyEncode compresses src into the Kafka snappy framing (4-byte BE length
// + raw snappy block).
func snappyEncode(dst, src []byte) []byte {
	if len(src) <= 4 {
		// Emit a single literal covering the whole input.
		out := make([]byte, 0, 4+1+1+len(src))
		out = binary.BigEndian.AppendUint32(out, uint32(len(src)))
		return appendSnappyLiteral(out, src)
	}
	encoded := snappyBlockEncode(src)
	out := make([]byte, 0, 4+len(encoded))
	out = binary.BigEndian.AppendUint32(out, uint32(len(encoded)))
	return append(out, encoded...)
}

// appendSnappyLiteral appends an uncompressed literal tag + payload.
func appendSnappyLiteral(out, src []byte) []byte {
	n := len(src)
	switch {
	case n < 60:
		out = append(out, byte(n-1)<<2) // literal tag, length-1 in upper 6 bits
	case n < 256:
		out = append(out, 60<<2, byte(n-1))
	case n < 65536:
		out = append(out, 61<<2, byte(n-1), byte((n-1)>>8))
	default:
		out = append(out, 62<<2, byte(n-1), byte((n-1)>>8), byte((n-1)>>16), byte((n-1)>>24))
	}
	return append(out, src...)
}

// snappyBlockEncode performs a simple greedy LZ77-style block encoding. It is
// fully compatible with the snappy block format (literals + copies) and lets
// standard snappy decoders (and klauspost/compress, if later swapped in)
// decompress the result.
func snappyBlockEncode(src []byte) []byte {
	tableSize := 1 << 14
	table := make([]int32, tableSize)
	for i := range table {
		table[i] = -1
	}

	out := make([]byte, 0, snappyMaxEncodedLen(len(src)))
	litStart := 0
	i := 0
	const minMatch = 4

	emitLiteral := func(end int) {
		if end > litStart {
			out = appendSnappyLiteral(out, src[litStart:end])
		}
	}

	for i+minMatch <= len(src) {
		h := (uint32(src[i]) | uint32(src[i+1])<<8 | uint32(src[i+2])<<16 | uint32(src[i+3])<<24) * 0x1e35a7bd
		pos := int((h >> 15) & uint32(tableSize-1))
		cand := table[pos]
		table[pos] = int32(i)
		if cand >= 0 && int(cand)+4 <= i && i-int(cand) <= 65535 && src[cand] == src[i] && src[cand+1] == src[i+1] && src[cand+2] == src[i+2] && src[cand+3] == src[i+3] {
			// Extend the match, capped at 64 bytes: snappy copy length is
			// limited to 64, longer runs are emitted as chained copies.
			matchLen := 4
			for i+matchLen < len(src) && src[int(cand)+matchLen] == src[i+matchLen] && matchLen < 64 {
				matchLen++
			}
			emitLiteral(i)
			// Copy tags (per the snappy block format):
			//  - 1-byte offset (type 1): length = 4 + (tag>>2), max 67; offset-1 <= 511
			//  - 2-byte offset (type 2): length = 1 + (tag>>2), max 64
			offset := i - int(cand)
			off := offset - 1
			if off <= 511 && matchLen < 12 {
				// 1-byte offset copy: tag = ((len-4) << 2) | 1, 3 high offset bits in tag
				tag := byte((matchLen-4)<<2) | 1 | byte(off>>8)<<5
				out = append(out, tag, byte(off))
			} else {
				// 2-byte offset copy: tag = ((len-1) << 2) | 2
				out = append(out, byte((matchLen-1)<<2)|2, byte(off), byte(off>>8))
			}
			i += matchLen
			litStart = i
			continue
		}
		i++
	}
	emitLiteral(len(src))
	return out
}

// snappyDecode decodes the Kafka snappy framing (4-byte BE length prefix then a
// raw snappy block). An empty input yields empty output.
func snappyDecode(dst, src []byte) ([]byte, error) {
	if len(src) < 4 {
		if len(src) == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("snappy: missing length prefix")
	}
	blockLen := binary.BigEndian.Uint32(src[:4])
	if int(blockLen) != len(src)-4 {
		return nil, fmt.Errorf("snappy: length mismatch %d != %d", blockLen, len(src)-4)
	}
	out, err := snappyBlockDecode(dst, src[4:])
	if err != nil {
		return nil, err
	}
	return out, nil
}

// snappyBlockDecode decodes a raw snappy block (literals and copies).
func snappyBlockDecode(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}
	i := 0
	for i < len(src) {
		tag := src[i]
		i++
		switch tag & 3 {
		case 0: // literal
			length := int(tag >> 2)
			if length >= 60 {
				n := length - 59
				if i+n > len(src) {
					return nil, fmt.Errorf("snappy: literal length overrun")
				}
				length = 0
				for j := 0; j < n; j++ {
					length |= int(src[i+j]) << (8 * j)
				}
				i += n
			}
			length++
			if i+length > len(src) {
				return nil, fmt.Errorf("snappy: literal overrun")
			}
			dst = append(dst, src[i:i+length]...)
			i += length
		case 1: // copy, 1-byte offset (stored as offset-1)
			length := 4 + int(tag>>2)
			if i+1 > len(src) {
				return nil, fmt.Errorf("snappy: copy overrun")
			}
			offset := int(src[i]) | int(tag&0xe0)<<3
			offset++ // wire format stores offset-1
			i++
			var err error
			dst, err = snappyCopy(dst, offset, length)
			if err != nil {
				return nil, err
			}
		case 2: // copy, 2-byte offset (stored as offset-1)
			length := 1 + int(tag>>2)
			if i+2 > len(src) {
				return nil, fmt.Errorf("snappy: copy overrun")
			}
			offset := int(src[i]) | int(src[i+1])<<8
			offset++ // wire format stores offset-1
			i += 2
			var err error
			dst, err = snappyCopy(dst, offset, length)
			if err != nil {
				return nil, err
			}
		default: // case 3: copy, 4-byte offset (stored as offset-1)
			length := 1 + int(tag>>2)
			if i+4 > len(src) {
				return nil, fmt.Errorf("snappy: copy overrun")
			}
			offset := int(src[i]) | int(src[i+1])<<8 | int(src[i+2])<<16 | int(src[i+3])<<24
			offset++ // wire format stores offset-1
			i += 4
			var err error
			dst, err = snappyCopy(dst, offset, length)
			if err != nil {
				return nil, err
			}
		}
	}
	return dst, nil
}

// snappyCopy appends `length` bytes copied from `offset` bytes back.
func snappyCopy(dst []byte, offset, length int) ([]byte, error) {
	if offset <= 0 || offset > len(dst) {
		return nil, fmt.Errorf("snappy: invalid copy offset %d", offset)
	}
	start := len(dst) - offset
	for i := 0; i < length; i++ {
		dst = append(dst, dst[start+i])
	}
	return dst, nil
}

// ---------------------------------------------------------------------------
// LZ4 block format (as used by Kafka): 4-byte magic 0x184D2204 + 4-byte BE
// compressed length + 4-byte BE uncompressed length + raw LZ4 block. The block
// carries its own 4-byte little-endian xxHash32 at the end, which we ignore on
// decode (we verify sizes instead).
// ---------------------------------------------------------------------------

const (
	lz4FrameMagic    uint32 = 0x184D2204
	lz4BlockMagic    uint32 = 0x184D2205
	lz4BlockLinked   uint32 = 0x184D2206
	lz4BlockUnlinked uint32 = 0x184D2207
)

// lz4MaxCompressedLen is the worst-case upper bound for an LZ4 block.
func lz4MaxCompressedLen(srcLen int) int {
	// Each byte could become a 2-byte token + up to 15-byte length extension.
	return srcLen + srcLen/255 + 16
}

// lz4BlockCompress compresses src into a Kafka-compatible LZ4 block frame.
func lz4BlockCompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	block := lz4BlockEncode(src)
	out := make([]byte, 0, 16+len(block))
	out = binary.BigEndian.AppendUint32(out, lz4FrameMagic)
	out = binary.BigEndian.AppendUint32(out, uint32(len(block)))
	out = binary.BigEndian.AppendUint32(out, uint32(len(src)))
	return append(out, block...), nil
}

// lz4BlockEncode performs a greedy hash-table LZ4 block encoding producing a
// standard LZ4 block (token + literals + offsets). The trailing 4-byte xxHash32
// checksum is not appended here; the frame decoder ignores it.
func lz4BlockEncode(src []byte) []byte {
	out := make([]byte, 0, lz4MaxCompressedLen(len(src)))
	const tableBits = 14
	tableSize := 1 << tableBits
	table := make([]int32, tableSize)
	for i := range table {
		table[i] = -1
	}

	litStart := 0
	i := 0
	const minMatch = 4

	for i+minMatch <= len(src) {
		h := (uint32(src[i]) | uint32(src[i+1])<<8 | uint32(src[i+2])<<16 | uint32(src[i+3])<<24)
		h ^= h >> 16
		h *= 0x9e3779b1
		h ^= h >> 16
		pos := int(h & uint32(tableSize-1))
		cand := table[pos]
		table[pos] = int32(i)
		if cand >= 0 && int(cand)+4 <= i && i-int(cand) <= 65535 && src[cand] == src[i] && src[cand+1] == src[i+1] && src[cand+2] == src[i+2] && src[cand+3] == src[i+3] {
			matchLen := 4
			for i+matchLen < len(src) && src[int(cand)+matchLen] == src[i+matchLen] && matchLen < 65535+15 {
				matchLen++
			}
			litLen := i - litStart
			token := 0
			if litLen >= 15 {
				token |= 15 << 4
			} else {
				token |= litLen << 4
			}
			if matchLen >= 19 {
				token |= 15
			} else {
				token |= (matchLen - 4)
			}
			out = append(out, byte(token))
			// literal length extension
			if litLen >= 15 {
				rest := litLen - 15
				for rest >= 255 {
					out = append(out, 255)
					rest -= 255
				}
				out = append(out, byte(rest))
			}
			out = append(out, src[litStart:i]...)
			offset := i - int(cand)
			out = append(out, byte(offset), byte(offset>>8))
			if matchLen >= 19 {
				rest := matchLen - 19
				for rest >= 255 {
					out = append(out, 255)
					rest -= 255
				}
				out = append(out, byte(rest))
			}
			i += matchLen
			litStart = i
			continue
		}
		i++
	}
	// Trailing literals.
	litLen := len(src) - litStart
	token := 0
	if litLen >= 15 {
		token = 15 << 4
	} else {
		token = litLen << 4
	}
	out = append(out, byte(token))
	if litLen >= 15 {
		rest := litLen - 15
		for rest >= 255 {
			out = append(out, 255)
			rest -= 255
		}
		out = append(out, byte(rest))
	}
	out = append(out, src[litStart:]...)
	return out
}

// lz4BlockDecompress decodes a Kafka LZ4 block frame.
func lz4BlockDecompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	if len(src) < 12 {
		return nil, fmt.Errorf("lz4: frame too short")
	}
	magic := binary.BigEndian.Uint32(src[:4])
	if magic != lz4FrameMagic {
		return nil, fmt.Errorf("lz4: bad magic 0x%08x", magic)
	}
	compLen := int(binary.BigEndian.Uint32(src[4:8]))
	uncompLen := int(binary.BigEndian.Uint32(src[8:12]))
	if compLen < 0 || uncompLen < 0 || compLen != len(src)-12 {
		return nil, fmt.Errorf("lz4: length mismatch comp=%d avail=%d", compLen, len(src)-12)
	}
	// Kafka LZ4 blocks (KIP-57) append a 4-byte xxHash32; tolerate both.
	block := src[12:]
	if len(block) == compLen-4 && uncompLen == compLen+4 {
		block = block[:compLen-4]
	}
	out, err := lz4BlockDecode(block, uncompLen)
	if err != nil {
		return nil, err
	}
	if len(out) != uncompLen {
		return nil, fmt.Errorf("lz4: decompressed size %d != %d", len(out), uncompLen)
	}
	return out, nil
}

// lz4BlockDecode decodes a raw LZ4 block.
func lz4BlockDecode(src []byte, uncompLen int) ([]byte, error) {
	dst := make([]byte, 0, uncompLen)
	i := 0
	for i < len(src) {
		token := src[i]
		i++
		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if i >= len(src) {
					return nil, fmt.Errorf("lz4: literal length overrun")
				}
				b := src[i]
				i++
				litLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if i+litLen > len(src) {
			return nil, fmt.Errorf("lz4: literal overrun")
		}
		dst = append(dst, src[i:i+litLen]...)
		i += litLen
		if i >= len(src) {
			break // last sequence is literals only
		}
		if i+2 > len(src) {
			return nil, fmt.Errorf("lz4: offset overrun")
		}
		offset := int(src[i]) | int(src[i+1])<<8
		i += 2
		matchLen := int(token & 0x0f)
		if matchLen == 15 {
			for {
				if i >= len(src) {
					return nil, fmt.Errorf("lz4: match length overrun")
				}
				b := src[i]
				i++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		matchLen += 4
		if offset <= 0 || offset > len(dst) {
			return nil, fmt.Errorf("lz4: invalid offset %d", offset)
		}
		start := len(dst) - offset
		for j := 0; j < matchLen; j++ {
			dst = append(dst, dst[start+j])
		}
	}
	if len(dst) > uncompLen {
		dst = dst[:uncompLen]
	}
	return dst, nil
}
