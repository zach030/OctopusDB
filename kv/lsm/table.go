package lsm

import (
	"encoding/binary"
	"os"
	"sync/atomic"
	"time"

	file2 "github.com/zach030/OctopusDB/kv/file"
	"github.com/zach030/OctopusDB/kv/pb"
	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/prometheus/common/log"

	"github.com/pkg/errors"
)

// table 对应磁盘的sst文件，在内存中的操作集合
type table struct {
	manager *LevelManager
	sst     *file2.SSTable
	fid     uint64
	ref     int32
}

// openTable with builder argument
// 1. builder is nil: sst file is already exist, openTable intend to load sst-file in disk and set sst options
// 2. builder is not nil: immutable is ready to flush, so new sst file and iterate all entries for flushing
func openTable(manager *LevelManager, sstName string, builder *tableBuilder) *table {
	sstSize := int(manager.cfg.SSTableMaxSz)
	if builder != nil {
		sstSize = builder.done().size
	}
	var (
		t   *table
		err error
	)
	fid := utils2.FID(sstName)
	// builder exist, need to flush to disk
	if builder != nil {
		if t, err = builder.flush(manager, sstName); err != nil {
			log.Error("table-builder flush sst failed,err:", err)
			return nil
		}
	} else {
		t = &table{manager: manager, fid: fid}
		t.sst = file2.OpenSSTable(&file2.Option{
			FID:      fid,
			FileName: sstName,
			Dir:      manager.cfg.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    sstSize,
		})
	}
	t.IncrRef()
	if err := t.sst.Init(); err != nil {
		log.Error("init sst file failed,err:", err)
		return nil
	}
	// 获取sst的最大key 需要使用迭代器
	itr := t.NewIterator(&utils2.Options{IsAsc: true}) // 默认是降序
	defer itr.Close()
	// 定位到初始位置就是最大的key
	itr.Rewind()
	if !itr.Valid() {
		panic(errors.Errorf("failed to read index, form maxKey"))
	}
	maxKey := itr.Item().Entry().Key
	t.sst.SetMaxKey(maxKey)
	return t
}

// Search 在table内查找key所对应的entry
func (t *table) Search(key []byte, maxVersion *uint64) (*utils2.Entry, error) {
	t.IncrRef()
	defer t.DecrRef()
	bloomFilter := utils2.BloomFilter{Filter: key}
	// 1.先走布隆过滤器查
	if t.sst.HasBloomFilter(); !bloomFilter.MayContain(key) {
		return nil, errors.New("key not found")
	}
	iter := t.NewIterator(&utils2.Options{})
	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils2.ErrKeyNotExist
	}
	found := iter.Item().Entry()
	if utils2.IsSameKey(key, found.Key) {
		if version := utils2.ParseTimeStamp(found.Key); *maxVersion < version {
			*maxVersion = version
			return found, nil
		}
	}
	return nil, utils2.ErrKeyNotExist
}

// block 根据索引在sst中构建block
func (t *table) block(idx int) (*block, error) {
	if idx < 0 {
		panic(errors.Errorf("id=%d", idx))
	}
	if idx >= len(t.sst.Index().GetOffsets()) {
		return nil, errors.Errorf("block:%d out of index", idx)
	}
	var (
		b   = &block{}
		err error
	)
	// query cache
	key := t.FormatCacheKey(idx)
	// cache hit
	blk, ok := t.manager.cache.blocks.Get(key)
	if ok {
		if bl, ok1 := blk.(*block); ok1 {
			return bl, nil
		}
	}
	// cache miss
	blockOffset, ok := t.getBlockOffset(idx)
	if !ok {
		panic("block offset panic")
	}
	b.offset = int(blockOffset.GetOffset())
	if b.data, err = t.read(b.offset, int(blockOffset.Len)); err != nil {
		return nil, errors.Wrapf(err, "read sst:%d, offset:%d, size:%d failed", t.fid, b.offset, blockOffset.Len)
	}
	// decode block from data buf
	if err = b.Decode(); err != nil {
		return nil, err
	}
	t.manager.cache.blocks.Set(key, b)
	return b, nil
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

func (t *table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

// getBlockOffset get blockOffset
func (t *table) getBlockOffset(idx int) (*pb.BlockOffset, bool) {
	tblIdx := t.sst.Index()
	if idx < 0 || idx >= len(tblIdx.Offsets) {
		return nil, false
	}
	off := tblIdx.GetOffsets()[idx]
	return off, true
}

// FormatCacheKey generate key in cache: fid+index
func (t *table) FormatCacheKey(idx int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

// GetCreatedAt
func (t *table) GetCreatedAt() *time.Time {
	return t.sst.GetCreatedAt()
}
