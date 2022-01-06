package utils

import "time"

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
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
