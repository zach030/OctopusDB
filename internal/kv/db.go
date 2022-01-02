package kv

import (
	"github.com/zach030/OctopusDB/internal/kv/lsm"
	"github.com/zach030/OctopusDB/internal/kv/vlog"
)

type OctopusDB struct {
	lsm  *lsm.LSM
	vlog *vlog.Vlog
	opt  *Options
}

func Open(opt *Options) *OctopusDB {
	db := &OctopusDB{opt: opt}
	db.lsm = lsm.NewLSM(&lsm.Config{
		WorkDir:            opt.WorkDir,
		MemTableSize:       opt.MemTableSize,
		SSTableMaxSz:       opt.SSTableMaxSz,
		BlockSize:          4 * 1024,
		BloomFalsePositive: 0.01,
	})
	return db
}
