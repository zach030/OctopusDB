package utils

import (
	"bytes"
	"encoding/binary"
)

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	ExpiresAt uint64
}

func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

func WalCodec(buf *bytes.Buffer, entry *Entry) int {
	return 0
}
