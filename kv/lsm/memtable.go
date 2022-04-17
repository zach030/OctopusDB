package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	file2 "github.com/zach030/OctopusDB/kv/file"
	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/pkg/errors"
)

const (
	walFileExt = ".wal"
)

type MemTable struct {
	lsm        *LSM
	wal        *file2.Wal
	skipList   *utils2.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemTable 当内存中memtable已满时，创建新的内存索引
func (l *LSM) NewMemTable() *MemTable {
	fid := atomic.AddUint64(&l.maxMemFd, 1)
	opt := &file2.Option{
		FID:      fid,
		Dir:      l.cfg.WorkDir,
		FileName: memTableFilePath(l.cfg.WorkDir, fid),
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(l.cfg.MemTableSize),
	}
	mt := &MemTable{
		wal:      file2.OpenWalFile(opt),
		lsm:      l,
		skipList: utils2.NewSkipList(int64(1 << 20)),
	}
	return mt
}

// openMemTable 通过读文件构建内存索引
func (l *LSM) openMemTable(fid uint64) *MemTable {
	opt := &file2.Option{
		FID:      fid,
		FileName: memTableFilePath(l.cfg.WorkDir, fid),
		Dir:      l.cfg.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(l.cfg.MemTableSize),
	}
	s := utils2.NewSkipList(int64(1 << 20))
	mt := &MemTable{
		skipList: s,
		buf:      &bytes.Buffer{},
		lsm:      l,
	}
	mt.wal = file2.OpenWalFile(opt)
	if err := mt.refreshSkipList(); err != nil {
		return nil
	}
	return mt
}

func (m *MemTable) Set(entry *utils2.Entry) error {
	// 先写wal
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 再写内存跳表
	m.skipList.Add(entry)
	return nil
}

func (m *MemTable) Get(key []byte) (*utils2.Entry, error) {
	vs := m.skipList.Search(key)

	e := &utils2.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}
	return e, nil
}

// refreshSkipList read and iterate wal file, fetch entry and add to skiplist
func (m *MemTable) refreshSkipList() error {
	if m.wal == nil || m.skipList == nil {
		return errors.New("nil wal or skiplist")
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.cfg))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	return m.wal.Truncate(int64(endOff))
}

func (m *MemTable) Close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	return nil
}

func (m *MemTable) replayFunction(opt *Config) func(*utils2.Entry, *utils2.ValuePtr) error {
	return func(e *utils2.Entry, _ *utils2.ValuePtr) error { // Function for replaying.
		if ts := utils2.ParseTimeStamp(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.skipList.Add(e)
		return nil
	}
}

func memTableFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
