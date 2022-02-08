package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/zach030/OctopusDB/internal/kv/file"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

const (
	walFileExt = ".wal"
)

type MemTable struct {
	lsm        *LSM
	wal        *file.Wal
	skipList   *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemTable 当内存中memtable已满时，创建新的内存索引
func (l *LSM) NewMemTable() *MemTable {
	fid := atomic.AddUint64(&l.maxMemFd, 1)
	opt := &file.Option{
		FID:      fid,
		Dir:      l.cfg.WorkDir,
		FileName: memTableFilePath(l.cfg.WorkDir, fid),
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(l.cfg.MemTableSize),
	}
	mt := &MemTable{
		wal:      file.OpenWalFile(opt),
		lsm:      l,
		skipList: utils.NewSkipList(int64(1 << 20)),
	}
	return mt
}

// openMemTable 通过读文件构建内存索引
func (l *LSM) openMemTable(fid uint64) *MemTable {
	opt := &file.Option{
		FID:      fid,
		FileName: memTableFilePath(l.cfg.WorkDir, fid),
		Dir:      l.cfg.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(l.cfg.MemTableSize),
	}
	s := utils.NewSkipList(int64(1 << 20))
	mt := &MemTable{
		skipList: s,
		buf:      &bytes.Buffer{},
		lsm:      l,
	}
	mt.wal = file.OpenWalFile(opt)
	if err := mt.refreshSkipList(); err != nil {
		return nil
	}
	return mt
}

func (m *MemTable) Set(entry *utils.Entry) error {
	// 先写wal
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 再写内存跳表
	if err := m.skipList.Add(entry); err != nil {
		return err
	}
	return nil
}

func (m *MemTable) Get(key []byte) *utils.Entry {
	return m.skipList.Search(key)
}

func (m *MemTable) refreshSkipList() error {
	if m.wal == nil || m.skipList == nil {
		return errors.New("")
	}

	// todo 遍历wal文件，写入skiplist
	return nil
}

func (m *MemTable) Close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.skipList.Close(); err != nil {
		return err
	}
	return nil
}

func (m *MemTable) replayFunction(opt *Config) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error { // Function for replaying.
		if ts := utils.ParseTimeStamp(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		return m.skipList.Add(e)
	}
}

func memTableFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
