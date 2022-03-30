package lsm

import (
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/zach030/OctopusDB/internal/kv/file"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type LevelManager struct {
	lsm           *LSM
	manifestFile  *file.ManifestFile // 记录level信息
	cfg           *Config
	levels        []*levelHandler // 对每个level操作的handler
	cache         *cache          // 缓存
	maxFID        uint64          // 当前最大fileID
	compactStatus *compactStatus
}

func (l *LSM) initLevelManager(cfg *Config) *LevelManager {
	lm := &LevelManager{
		lsm: l,
		cfg: cfg,
	}
	lm.compactStatus = l.newCompactStatus()
	// 加载manifest文件
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	// 根据manifest文件构建levelHandler
	if err := lm.build(); err != nil {
		panic(err)
	}
	return lm
}

func (l *LevelManager) loadManifest() error {
	mf, err := file.OpenManifestFile(&file.Option{Dir: l.lsm.cfg.WorkDir})
	if err != nil {
		return err
	}
	l.manifestFile = mf
	return nil
}

func (l *LevelManager) build() error {
	l.levels = make([]*levelHandler, 0, l.cfg.MaxLevelNum)
	for i := 0; i < l.cfg.MaxLevelNum; i++ {
		l.levels = append(l.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			manager:  l,
		})
	}
	// 从manifest中获取数据来构建
	manifest := l.manifestFile.Manifest()
	if err := l.manifestFile.FilterValidTables(utils.LoadIDMap(l.cfg.WorkDir)); err != nil {
		return err
	}
	l.cache = NewCache(l.cfg)
	var maxFd uint64
	for tid, tableManifest := range manifest.Tables {
		if tid > maxFd {
			maxFd = tid
		}
		filename := utils.SSTFullFileName(l.cfg.WorkDir, tid)
		t := openTable(l, filename, nil)
		l.levels[tableManifest.Level].tables = append(l.levels[tableManifest.Level].tables, t)
	}
	for i := 0; i < l.cfg.MaxLevelNum; i++ {
		l.levels[i].Sort()
	}
	atomic.AddUint64(&l.maxFID, maxFd)
	return nil
}

// flush if memtable size over limit, flush it to disk L0 as sst file
func (l *LevelManager) flush(immutable *MemTable) error {
	// 获取fid，构造sst文件名
	fid := immutable.wal.FID()
	sstName := utils.SSTFullFileName(l.cfg.WorkDir, fid)
	log.Info("[Flush] immutable file to sst:", sstName)
	// 构建table-builder
	builder := newTableBuilder(l.cfg)
	// 将所有entry加入到builder
	iter := immutable.skipList.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	tbl := openTable(l, sstName, builder)
	// 更新manifest
	if err := l.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		CheckSum: []byte{'m', 'o', 'c', 'k'},
	}); err != nil {
		panic(err)
	}
	// flush to level-0
	l.levels[0].add(tbl)
	return nil
}

// Get query key in sst files from L0-L7
func (l *LevelManager) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)
	// l0 层查询
	if entry, err = l.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// l1-l7 层查询
	for i := 0; i < l.cfg.MaxLevelNum; i++ {
		level := l.levels[i]
		if entry, err = level.Get(key); entry != nil {
			return entry, err
		}
	}
	return nil, errors.New("key not found")
}

func (l *LevelManager) lastLevelSize() int64 {
	return l.lastLevel().levelSize()
}

func (l *LevelManager) lastLevel() *levelHandler {
	return l.levels[len(l.levels)-1]
}

func (h *levelHandler) levelSize() int64 {
	return h.totalSize
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (h *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	// 找到与kr有重合的tables索引左右边界
	left := sort.Search(len(h.tables), func(i int) bool {
		return utils.CompareKeys(kr.left, h.tables[i].sst.MaxKey()) <= 0
	})
	right := sort.Search(len(h.tables), func(i int) bool {
		return utils.CompareKeys(kr.right, h.tables[i].sst.MaxKey()) < 0
	})
	return left, right
}

func (l *LevelManager) close() error {
	if err := l.cache.close(); err != nil {
		return err
	}
	if err := l.manifestFile.Close(); err != nil {
		return err
	}
	for i := range l.levels {
		if err := l.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}
