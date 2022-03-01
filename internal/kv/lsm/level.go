package lsm

import (
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/zach030/OctopusDB/internal/kv/file"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type LevelManager struct {
	lsm          *LSM
	manifestFile *file.ManifestFile
	cfg          *Config
	levels       []*levelHandler
	cache        *cache
	maxFID       uint64
}

func (l *LSM) initLevelManager(cfg *Config) *LevelManager {
	lm := &LevelManager{
		lsm: l,
		cfg: cfg,
	}
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
	log.Info("success load manifest-file")
	return nil
}

func (l *LevelManager) build() error {
	l.levels = make([]*levelHandler, 0, l.cfg.MaxLevelNums)
	for i := 0; i < len(l.levels); i++ {
		l.levels = append(l.levels, &levelHandler{
			level:   0,
			tables:  make([]*table, 0),
			manager: l,
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
	for i := 0; i < l.cfg.MaxLevelNums; i++ {
		l.levels[i].Sort()
	}
	atomic.AddUint64(&l.maxFID, maxFd)
	log.Info("success build level-manager")
	return nil
}

// flush if memtable size over limit, flush it to disk L0 as sst file
func (l *LevelManager) flush(immutable *MemTable) error {
	// 分配一个fid
	fid := immutable.wal.FID()
	sstName := utils.SSTFullFileName(l.cfg.WorkDir, fid)
	fmt.Println("flush immutable file to sst:", sstName)
	// 构建table-builder
	builder := newTableBuilder(l.cfg)
	// 将所有entry加入到builder
	iter := immutable.skipList.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item()
		builder.add(entry.Entry(), false)
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
	for i := 0; i < l.cfg.MaxLevelNums; i++ {
		level := l.levels[i]
		if entry, err = level.Get(key); entry != nil {
			return entry, err
		}
	}
	return nil, errors.New("key not found")
}
