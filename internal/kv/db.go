package kv

import (
	"github.com/zach030/OctopusDB/internal/kv/lsm"
	"github.com/zach030/OctopusDB/internal/kv/vlog"
)

type OctopusDB struct {
	lsm  *lsm.LSM
	vlog *vlog.Vlog
}

func Open() *OctopusDB {
	return &OctopusDB{}
}
