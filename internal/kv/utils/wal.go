package utils

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
)

const maxHeaderSize int = 21

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	ExpiresAt uint64
}

// Encode format: | header | key | value | crc32 |
func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}
	hash := crc32.New(CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)
	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	if _, err := writer.Write(headerEnc[:sz]); err != nil {
		panic(err)
	}
	if _, err := writer.Write(e.Key); err != nil {
		panic(err)
	}
	if _, err := writer.Write(e.Value); err != nil {
		panic(err)
	}
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	if _, err := buf.Write(crcBuf[:]); err != nil {
		panic(err)
	}
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

type LogEntry func(e *Entry, vp *ValuePtr) error
