package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/prometheus/common/log"

	"github.com/pkg/errors"
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

func (w *Wal) Write(entry *utils2.Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	// 将entry序列化,存到buf中 获取编码后的长度n
	n := utils2.WalCodec(w.buf, entry)
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
func (w *Wal) Iterate(readonly bool, offset uint32, fn utils2.LogEntry) (uint32, error) {
	reader := bufio.NewReader(w.file.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           w,
	}
	var validEndOffset = offset
loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils2.ErrTruncate:
			break loop
		case e.IsZero():
			break loop
		}
		var vp utils2.ValuePtr // 给kv分离的设计留下扩展,可以不用考虑其作用
		size := uint32(e.LogHeaderLen() + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils2.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validEndOffset, nil
}

type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *Wal
}

func (r *SafeRead) MakeEntry(reader io.Reader) (*utils2.Entry, error) {
	hashReader := utils2.NewHashReader(reader)
	var h utils2.WalHeader
	hlen, err := h.Decode(hashReader)
	if err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, utils2.ErrTruncate
	}
	// header + key + val + crc
	kl, vl := int(h.KeyLen), int(h.ValueLen)
	e := &utils2.Entry{
		Offset: r.RecordOffset,
		Hlen:   hlen,
	}
	kvBuf := make([]byte, kl+vl)
	if _, err := io.ReadFull(hashReader, kvBuf); err != nil {
		if err == io.EOF {
			err = utils2.ErrTruncate
		}
		return nil, err
	}
	e.Key, e.Value = kvBuf[:kl], kvBuf[kl:]
	crcBuf := make([]byte, crc32.Size)
	if _, err := io.ReadFull(reader, crcBuf); err != nil {
		if err == io.EOF {
			err = utils2.ErrTruncate
		}
		return nil, err
	}
	crc := utils2.BytesToU32(crcBuf[:])
	if crc != hashReader.Sum32() {
		log.Error(err)
		return nil, utils2.ErrTruncate
	}
	e.ExpiresAt = h.ExpiresAt
	return e, nil
}

func (w *Wal) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	if fi, err := w.file.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v\n", w.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	w.size = uint32(end)
	return w.file.Truncate(end)
}
