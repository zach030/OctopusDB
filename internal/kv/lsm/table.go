package lsm

import (
	"os"
	"sync/atomic"

	"github.com/prometheus/common/log"

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

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t tableIterator) Next() {
	panic("implement me")
}

func (t tableIterator) Rewind() {
	panic("implement me")
}

func (t tableIterator) Valid() bool {
	panic("implement me")
}

func (t tableIterator) Close() error {
	panic("implement me")
}

func (t tableIterator) Seek(bytes []byte) {
	panic("implement me")
}

func (t tableIterator) Item() utils.Item {
	panic("implement me")
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}

func openTable(manager *LevelManager, sstName string, builder *tableBuilder) *table {
	sstSize := int(manager.cfg.SSTableMaxSz)
	if builder != nil {
		sstSize = builder.done().size
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(sstName)

	if builder != nil {
		if t, err = builder.flush(manager, sstName); err != nil {
			log.Error("table-builder flush sst failed,err:", err)
			return nil
		}
	} else {
		t = &table{manager: manager, fid: fid}
		t.sst = file.OpenSSTable(&file.Option{
			FID:      fid,
			FileName: sstName,
			Dir:      manager.cfg.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(manager.cfg.SSTableMaxSz),
		})
	}
	t.IncrRef()
	if err := t.sst.Init(); err != nil {
		log.Error("init sst file failed,err:", err)
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

func (t *table) DecrRef() error {
	return nil
}

func (t *table) Size() int64 {
	return t.sst.Size()
}

func (t *table) StaleSize() uint32 {
	return t.sst.Index().StaleDataSize
}

func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
