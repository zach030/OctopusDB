package lsm

import (
	"os"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/zach030/OctopusDB/internal/kv/file"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

// table 对应磁盘的sst文件，在内存中的操作集合
type table struct {
	manager *LevelManager
	sst     *file.SSTable
	fid     uint64
	ref     int32
}

type tableBuilder struct {
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	// bi       *blockIterator
	err error
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
func (t *table) Search(key []byte, maxVersion *uint64) (*utils.Entry, error) {
	t.IncrRef()
	defer t.DecrRef()
	bloomFilter := utils.BloomFilter{Filter: key}
	// 1.先走布隆过滤器查
	if t.sst.HasBloomFilter(); !bloomFilter.MayContain(key) {
		return nil, errors.New("key not found")
	}
	return nil, nil
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *table) DecrRef() {

}
