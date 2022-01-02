package file

import (
	"bytes"
	"os"
	"sync"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type Wal struct {
	file *MmapFile
	lock sync.RWMutex
	opt  *Option
	buf  *bytes.Buffer

	size    uint32
	writeAt uint32
}

func OpenWalFile(opt *Option) *Wal {
	mf, err := OpenMmapFileWithName(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return nil
	}
	wal := &Wal{file: mf, opt: opt}
	wal.buf = &bytes.Buffer{}
	wal.size = uint32(len(wal.file.Data))
	return wal
}

func (w *Wal) Write(entry *utils.Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	// 将entry序列化
	n := utils.WalCodec(w.buf, entry)
	buf := w.buf.Bytes()
	if err := w.file.AppendBuffer(w.writeAt, buf); err != nil {
		panic(err)
	}
	w.writeAt += uint32(n)
	// 将处理后的entry放入buf
	return nil
}
