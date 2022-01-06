package lsm

import (
	"io/ioutil"
	"sort"
	"strconv"
	"strings"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type LSM struct {
	memTable   *MemTable
	imMemTable []*MemTable
	cfg        *Config
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
}

func NewLSM(cfg *Config) *LSM {
	lsm := &LSM{cfg: cfg}
	lsm.memTable, lsm.imMemTable = lsm.recover()
	return lsm
}

func (l *LSM) Close() error {
	return nil
}

func (l *LSM) Set(entry *utils.Entry) error {
	// 判断memtable是否超过大小，需要关闭，新建memtable
	if err := l.memTable.Set(entry); err != nil {
		return err
	}
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
	return nil, nil
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
	immt := make([]*MemTable, 0)
	for _, i := range fid {
		mt := l.openMemTable(i)
		immt = append(immt, mt)
	}
	return l.NewMemTable(), immt
}
