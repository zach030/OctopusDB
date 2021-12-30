package lsm

import "github.com/zach030/OctopusDB/internal/kv/utils"

type LSM struct {
	memTable   *MemTable
	imMemTable []*MemTable
	cfg        *Config
	maxMemFd   uint32
}

type Config struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64
}

func NewLSM() *LSM {
	return &LSM{}
}

func (l *LSM) Close() error {
	return nil
}

func (l *LSM) Set(entry *utils.Entry) error {
	return nil
}

func (l *LSM) Get(key []byte) (*utils.Entry, error) {
	// 先查内存
	if e := l.memTable.Get(key); e != nil {
		return e, nil
	}
	for _, im := range l.imMemTable {
		if e := im.Get(key); e != nil {
			return e, nil
		}
	}
	return nil, nil
}
