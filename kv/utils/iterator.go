package utils

type Iterator interface {
	Next()
	Rewind()
	Valid() bool
	Close() error
	Seek([]byte)
	Item() Item
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
