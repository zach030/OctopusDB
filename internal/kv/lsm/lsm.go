package lsm

import (
	"io/ioutil"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/common/log"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type LSM struct {
	memTable   *MemTable
	imMemTable []*MemTable
	cfg        *Config
	levels     *LevelManager
	maxMemFd   uint64
}

type Config struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64
	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
}

func NewLSM(cfg *Config) *LSM {
	lsm := &LSM{cfg: cfg}
	lsm.levels = lsm.initLevelManager(cfg)
	lsm.memTable, lsm.imMemTable = lsm.recover()
	return lsm
}

func (l *LSM) Close() error {
	return nil
}

func (l *LSM) Set(entry *utils.Entry) error {
	// 判断memtable是否超过大小，需要关闭，新建memtable
	if int64(l.memTable.wal.Size()) > l.cfg.MemTableSize {
		l.imMemTable = append(l.imMemTable, l.memTable)
		l.memTable = l.NewMemTable()
		log.Info("memtable size is over limit, success new a memetable")
	}
	if err := l.memTable.Set(entry); err != nil {
		return err
	}
	// 将已经不变的immt刷到disk，成为sst文件
	for _, immt := range l.imMemTable {
		if err := l.levels.flush(immt); err != nil {
			return err
		}
		if err := immt.Close(); err != nil {
			return err
		}
	}
	l.imMemTable = make([]*MemTable, 0)
	return nil
}

func (l *LSM) Get(key []byte) (*utils.Entry, error) {
	// 先查mt
	if e := l.memTable.Get(key); e != nil {
		return e, nil
	}
	// 再查imt
	for _, im := range l.imMemTable {
		if e := im.Get(key); e != nil {
			return e, nil
		}
	}
	return l.levels.Get(key)
}

func (l *LSM) StartCompaction() {
	// todo 对sst文件进行合并
}

// recover 重新构建内存索引
func (l *LSM) recover() (*MemTable, []*MemTable) {
	fs, err := ioutil.ReadDir(l.cfg.WorkDir)
	if err != nil {
		return nil, nil
	}
	log.Info("read files in current dir:", fs)
	fid := make([]uint64, 0)
	for _, f := range fs {
		if !strings.HasSuffix(f.Name(), walFileExt) {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(f.Name(), walFileExt), 10, 64)
		if err != nil {
			panic(err)
		}
		fid = append(fid, uint64(id))
	}
	sort.Slice(fid, func(i, j int) bool {
		return fid[i] < fid[j]
	})
	log.Info("find wal files:", fid)
	immt := make([]*MemTable, 0)
	for _, i := range fid {
		mt := l.openMemTable(i)
		immt = append(immt, mt)
	}
	log.Info("success load wal files and rebuild memtables and immtables")
	return l.NewMemTable(), immt
}
