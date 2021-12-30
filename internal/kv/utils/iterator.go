package utils

type Iterator interface {
	Next()
	Rewind()
	Valid()bool
	Close() error
	Seek([]byte)
}
