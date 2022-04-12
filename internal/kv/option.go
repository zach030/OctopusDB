package kv

import "github.com/zach030/OctopusDB/internal/kv/utils"

// Options corekv 总的配置文件
type Options struct {
	ValueThreshold      int64
	WorkDir             string
	MemTableSize        int64
	SSTableMaxSz        int64
	ValueLogFileSize    int
	MaxBatchCount       int64
	MaxBatchSize        int64
	ValueLogMaxEntries  uint32
	VerifyValueChecksum bool
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:          "./data",
		MemTableSize:     1024,
		SSTableMaxSz:     1 << 30,
		ValueLogFileSize: 1024 * 10,
		ValueThreshold:   0,
		MaxBatchCount:    10000,
		MaxBatchSize:     1 << 20,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
