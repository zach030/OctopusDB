package kv

import "github.com/zach030/OctopusDB/internal/kv/utils"

// Options corekv 总的配置文件
type Options struct {
	ValueThreshold int64
	WorkDir        string
	MemTableSize   int64
	SSTableMaxSz   int64
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:      "./data",
		MemTableSize: 1024,
		SSTableMaxSz: 1 << 30,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
