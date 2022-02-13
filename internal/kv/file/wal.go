package file

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type Wal struct {
	file *MmapFile
	lock *sync.RWMutex
	opt  *Option
	buf  *bytes.Buffer

	size    uint32
	writeAt uint32
}

func OpenWalFile(opt *Option) *Wal {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return nil
	}
	wal := &Wal{file: mf, opt: opt, lock: &sync.RWMutex{}}
	wal.buf = &bytes.Buffer{}
	wal.size = uint32(len(wal.file.Data))
	return wal
}

func (w *Wal) Write(entry *utils.Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	// 将entry序列化,存到buf中 获取编码后的长度n
	n := utils.WalCodec(w.buf, entry)
	buf := w.buf.Bytes()
	// 将序列化后的buf写入mmap
	if err := w.file.AppendBuffer(w.writeAt, buf); err != nil {
		panic(err)
	}
	w.writeAt += uint32(n)
	return nil
}

func (w *Wal) Size() uint32 {
	return w.writeAt
}

func (w *Wal) FID() uint64 {
	return w.opt.FID
}

func (w *Wal) Name() string {
	return w.file.Fd.Name()
}

func (w *Wal) Close() error {
	fileName := w.file.Fd.Name()
	if err := w.file.Fd.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

// Iterate 从磁盘中遍历wal获取数据
// fn : 拿到entry后做的工作
func (w *Wal) Iterate(readonly bool, offset uint32, fn utils.LogEntry) error {
	reader := bufio.NewReader(w.file.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           w,
	}
}

type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *Wal
}

func (r *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	hashReader := utils.NewHashReader(reader)
	var h utils.WalHeader
	hlen, err := h.Decode(hashReader)
	if err != nil {
		return nil, err
	}

}
