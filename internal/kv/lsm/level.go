package lsm

import (
	"fmt"
	"path/filepath"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type LevelManager struct {
	lsm *LSM
}

func (l *LSM) initLevelManager(cfg *Config) *LevelManager {
	lm := &LevelManager{
		lsm: l,
	}
	return lm
}

// flush if memtable size over limit, flush it to disk L0 as sst file
func (l *LevelManager) flush(immutable *MemTable) error {
	fid := immutable.wal.FID()
	sstName := filepath.Join(l.lsm.cfg.WorkDir, fmt.Sprintf("%05d.sst", fid))
	fmt.Println("new sst file:", sstName)
	iter := immutable.skipList.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		//
	}
	return nil
}

// Get query key in sst files from L0-L7
func (l *LevelManager) Get(key []byte) (*utils.Entry, error) {
	return nil, nil
}
