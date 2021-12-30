package utils

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64
}

func (e Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}
