package lsm

import (
	"os"

	"github.com/zach030/OctopusDB/internal/kv/file"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

// table 对应磁盘的sst文件，在内存中的操作集合
type table struct {
	manager *LevelManager
	sst     *file.SSTable
	fid     uint64
}

type tableBuilder struct {
}

func openTable(manager *LevelManager, sstName string, builder *tableBuilder) *table {
	t := &table{manager: manager}
	fid := utils.FID(sstName)
	t.sst = file.OpenSSTable(&file.Option{
		FID:      fid,
		FileName: sstName,
		Dir:      manager.cfg.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(manager.cfg.SSTableMaxSz),
	})
	if err := t.sst.Init(); err != nil {
		return nil
	}
	return t
}

// Search 在table内查找
func (t *table) Search() (*utils.Entry, error) {
	return nil, nil
}
