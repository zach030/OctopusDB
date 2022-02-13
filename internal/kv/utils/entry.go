package utils

import (
	"encoding/binary"
	"time"
)

// ValueStruct 将value与expiresAt作为一个整体
type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
}

// EncodedSize value只持久化具体的value值和过期时间
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value)
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// DecodeValue 反序列化到结构体
func (vs *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf)
	vs.Value = buf[sz:]
}

//EncodeValue 对value进行编码，并将编码后的字节写入byte
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	// 过期时间 | value
	sz := binary.PutUvarint(b[:], vs.ExpiresAt)
	n := copy(b[sz:], vs.Value)
	return uint32(sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}

func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) WithTTL(duration time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(duration).Unix())
	return e
}

func ValueSize(data []byte) int64 {
	return int64(len(data))
}

// IsZero _
func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}

// LogHeaderLen _
func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

// LogOffset _
func (e *Entry) LogOffset() uint32 {
	return e.Offset
}
