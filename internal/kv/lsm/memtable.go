package lsm

import (
	"bytes"
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
	lsm      *LSM
	wal      *file.Wal
	skipList *utils.SkipList
	buf      *bytes.Buffer
}

func (l *LSM) NewMemTable() *MemTable {
	fid := atomic.AddUint32(&l.maxMemFd, 1)
	opt := &file.Option{
		FID:   fid,
		Dir:   l.cfg.WorkDir,
		Path:  memTableFilePath(l.cfg.WorkDir, fid),
		Flag:  os.O_CREATE | os.O_RDWR,
		MaxSz: int(l.cfg.MemTableSize),
	}
	mt := &MemTable{
		wal:      file.OpenWalFile(opt),
		lsm:      l,
		skipList: utils.NewSkipList(),
	}
	return mt
}

func (m *MemTable) Set(entry *utils.Entry) error {
	// 先写wal
	if err := m.wal.Add(entry); err != nil {
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

func memTableFilePath(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
