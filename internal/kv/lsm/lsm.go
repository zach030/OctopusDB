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
	closer     *utils.Closer
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
	DiscardStatsCh      *chan map[uint32]int64
}

func NewLSM(cfg *Config) *LSM {
	lsm := &LSM{cfg: cfg}
	lsm.levels = lsm.initLevelManager(cfg)
	lsm.memTable, lsm.imMemTable = lsm.recover()
	// 初始化closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser()
	return lsm
}

func (l *LSM) Close() error {
	// 等待全部合并过程的结束
	// 等待全部api调用过程结束
	l.closer.Close()
	// TODO 需要加锁保证并发安全
	if l.memTable != nil {
		if err := l.memTable.Close(); err != nil {
			return err
		}
	}
	for i := range l.imMemTable {
		if err := l.imMemTable[i].Close(); err != nil {
			return err
		}
	}
	if err := l.levels.close(); err != nil {
		return err
	}
	return nil
}

func (l *LSM) Set(entry *utils.Entry) error {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 优雅关闭
	l.closer.Add(1)
	defer l.closer.Done()
	// 检查当前memtable是否写满，是的话创建新的memtable,并将当前内存表写到immutables中
	// 否则写入当前memtable中
	if int64(l.memTable.wal.Size())+
		int64(utils.EstimateWalCodecSize(entry)) > l.cfg.MemTableSize {
		l.Rotate()
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
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	l.closer.Add(1)
	defer l.closer.Done()
	// 先查mt
	var (
		entry *utils.Entry
		err   error
	)
	if entry, err = l.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}
	// 再查imt
	for _, im := range l.imMemTable {
		if entry, err = im.Get(key); err != nil {
			return nil, err
		}
	}
	return l.levels.Get(key)
}

func (l *LSM) StartCompaction() {
	// 初始化lsm时会启动NumCompactors个协程，用来后台执行合并压缩
	nums := l.cfg.NumCompactors
	l.closer.Add(nums)
	for i := 0; i < nums; i++ {
		go l.levels.runCompacter(i)
	}
}

func (l *LSM) Rotate() {
	l.imMemTable = append(l.imMemTable, l.memTable)
	l.memTable = l.NewMemTable()
}

// recover 重新构建内存索引
func (l *LSM) recover() (*MemTable, []*MemTable) {
	fs, err := ioutil.ReadDir(l.cfg.WorkDir)
	if err != nil {
		return nil, nil
	}
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
	log.Info("[Recover] find wal files:", fid)
	immt := make([]*MemTable, 0)
	for _, i := range fid {
		mt := l.openMemTable(i)
		immt = append(immt, mt)
	}
	return l.NewMemTable(), immt
}
