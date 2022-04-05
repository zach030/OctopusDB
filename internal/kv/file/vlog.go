package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/zach030/OctopusDB/internal/kv/utils"

	"github.com/pkg/errors"
)

type LogFile struct {
	Lock sync.RWMutex
	FID  uint32
	size uint32
	f    *MmapFile
}

// Open 打开vlog文件，这里底层也是通过mmap映射
func (lf *LogFile) Open(opt *Option) error {
	var err error
	lf.FID = uint32(opt.FID)
	lf.Lock = sync.RWMutex{}
	lf.f, err = OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		panic(err)
	}
	fi, err := lf.f.Fd.Stat()
	if err != nil {
		return errors.Wrap(err, "Unable to run file.Stat")
	}
	// 获取文件尺寸
	sz := fi.Size()
	if sz > math.MaxUint32 {
		panic(fmt.Errorf("file size: %d greater than %d", uint32(sz), uint32(math.MaxUint32)))
	}
	lf.size = uint32(sz)
	return nil
}

// Read Acquire lock on mmap/file if you are calling this
func (lf *LogFile) Read(p *utils.ValuePtr) (buf []byte, err error) {
	offset := p.Offset
	// Do not convert size to uint32, because the lf.fmap can be of size
	// 4GB, which overflows the uint32 during conversion to make the size 0,
	// causing the read to fail with ErrEOF. See issue #585.
	size := int64(len(lf.f.Data))
	valsz := p.Len
	lfsz := atomic.LoadUint32(&lf.size)
	if int64(offset) >= size || int64(offset+valsz) > size ||
		// Ensure that the read is within the file's actual size. It might be possible that
		// the offset+valsz length is beyond the file's actual size. This could happen when
		// dropAll and iterations are running simultaneously.
		int64(offset+valsz) > int64(lfsz) {
		err = io.EOF
	} else {
		buf, err = lf.f.Bytes(int(offset), int(valsz))
	}
	return buf, err
}

func (lf *LogFile) DoneWriting(offset uint32) error {
	// Sync before acquiring lock. (We call this from write() and thus know we have shared access
	// to the fd.)
	if err := lf.f.Sync(); err != nil {
		return errors.Wrapf(err, "Unable to sync value log: %q", lf.FileName())
	}

	lf.Lock.Lock()
	defer lf.Lock.Unlock()

	// TODO: Confirm if we need to run a file sync after truncation.
	// Truncation must run after unmapping, otherwise Windows would crap itself.
	if err := lf.f.Truncate(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.FileName())
	}

	// Reinitialize the log file. This will mmap the entire file.
	if err := lf.Init(); err != nil {
		return errors.Wrapf(err, "failed to initialize file %s", lf.FileName())
	}

	// Previously we used to close the file after it was written and reopen it in read-only mode.
	// We no longer open files in read-only mode. We keep all vlog files open in read-write mode.
	return nil
}

func (lf *LogFile) Write(offset uint32, buf []byte) (err error) {
	return lf.f.AppendBuffer(offset, buf)
}

func (lf *LogFile) Truncate(offset int64) error {
	return lf.f.Truncate(offset)
}

func (lf *LogFile) Close() error {
	return lf.f.Close()
}

func (lf *LogFile) Size() int64 {
	return int64(atomic.LoadUint32(&lf.size))
}

func (lf *LogFile) AddSize(offset uint32) {
	atomic.StoreUint32(&lf.size, offset)
}

func (lf *LogFile) Init() error {
	fstat, err := lf.f.Fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.FileName())
	}
	sz := fstat.Size()
	if sz == 0 {
		// File is empty. We don't need to mmap it. Return.
		return nil
	}
	if sz > math.MaxUint32 {
		panic(fmt.Errorf("[LogFile.Init] sz > math.MaxUint32"))
	}
	lf.size = uint32(sz)
	return nil
}

func (lf *LogFile) FileName() string {
	return lf.f.Fd.Name()
}

func (lf *LogFile) Seek(offset int64, whence int) (ret int64, err error) {
	return lf.f.Fd.Seek(offset, whence)
}

func (lf *LogFile) FD() *os.File {
	return lf.f.Fd
}

// You must hold lf.lock to sync()
func (lf *LogFile) Sync() error {
	return lf.f.Sync()
}

// EncodeEntry will encode entry to the buf
// layout of entry
// +--------+-----+-------+-------+
// | header | key | value | crc32 |
// +--------+-----+-------+-------+
func (lf *LogFile) EncodeEntry(e *utils.Entry, buf *bytes.Buffer, offset uint32) (int, error) {
	h := utils.Header{
		KLen:      uint32(len(e.Key)),
		VLen:      uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
		Meta:      e.Meta,
	}

	hash := crc32.New(utils.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [utils.MaxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	_, err := writer.Write(headerEnc[:sz])
	if err != nil {
		panic(err)
	}
	// Encryption is disabled so writing directly to the buffer.
	_, err = writer.Write(e.Key)
	if err != nil {
		panic(err)
	}
	_, err = writer.Write(e.Value)
	if err != nil {
		panic(err)
	}
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	_, err = buf.Write(crcBuf[:])
	if err != nil {
		panic(err)
	}
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}
func (lf *LogFile) DecodeEntry(buf []byte, offset uint32) (*utils.Entry, error) {
	var h utils.Header
	hlen := h.Decode(buf)
	kv := buf[hlen:]
	e := &utils.Entry{
		Meta:      h.Meta,
		ExpiresAt: h.ExpiresAt,
		Offset:    offset,
		Key:       kv[:h.KLen],
		Value:     kv[h.KLen : h.KLen+h.VLen],
	}
	return e, nil
}

// 完成log文件的初始化
func (lf *LogFile) Bootstrap() error {
	// TODO 是否需要初始化一些内容给vlog文件?
	return nil
}

func (lf *LogFile) Iterate(offset uint32, fn utils.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = utils.VlogHeaderSize
	}
	if int64(offset) == lf.Size() {
		// We're at the end of the file already. No need to do anything.
		return offset, nil
	}

	// We're not at the end of the file. Let's Seek to the offset and start reading.
	if _, err := lf.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "Unable to seek, name:%s", lf.FileName())
	}

	reader := bufio.NewReader(lf.FD())
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}

	var validEndOffset = offset

loop:
	for {
		e, err := read.Entry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e == nil:
			continue
		}

		var vp utils.ValuePtr
		vp.Len = uint32(e.Hlen + len(e.Key) + len(e.Value) + crc32.Size)
		read.recordOffset += vp.Len

		vp.Offset = e.Offset
		vp.Fid = lf.FID
		validEndOffset = read.recordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.Wrapf(err, fmt.Sprintf("Iteration function %s", lf.FileName()))
		}
	}
	return validEndOffset, nil
}

type safeRead struct {
	k            []byte
	v            []byte
	recordOffset uint32
	lf           *LogFile
}

// Entry reads an entry from the provided reader. It also validates the checksum for every entry
// read. Returns error on failure.
func (r *safeRead) Entry(reader io.Reader) (*utils.Entry, error) {
	tee := utils.NewHashReader(reader)
	var h utils.Header
	hlen, err := h.DecodeFrom(tee)
	if err != nil {
		return nil, err
	}
	if h.KLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, utils.ErrTruncate
	}
	kl := int(h.KLen)
	if cap(r.k) < kl {
		r.k = make([]byte, 2*kl)
	}
	vl := int(h.VLen)
	if cap(r.v) < vl {
		r.v = make([]byte, 2*vl)
	}

	e := &utils.Entry{}
	e.Offset = r.recordOffset
	e.Hlen = hlen
	buf := make([]byte, h.KLen+h.VLen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}

	e.Key = buf[:h.KLen]
	e.Value = buf[h.KLen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.Meta = h.Meta
	e.ExpiresAt = h.ExpiresAt
	return e, nil
}
