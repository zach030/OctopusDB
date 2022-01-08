package file

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/zach030/OctopusDB/internal/kv/utils"

	"github.com/golang/protobuf/proto"

	"github.com/zach030/OctopusDB/internal/kv/pb"
)

// SSTable construct of : block1 | block2 | ... | index_data | index_len | check_sum | check_sum_len
type SSTable struct {
	mf             *MmapFile      // mmap file for current table
	tableIdx       *pb.TableIndex // index record block metadata with: offset, size, start-key
	hasBloomFilter bool           // if contains bloom-filter
	idxStart       int            // start pos of index
	idxLen         int            // length of table index
	fid            uint64         // current sstable file id
	// store min/max key: faster to judge if the key is in current sst file
	minKey []byte // min key in current table
	maxKey []byte // max key for current table
}

func OpenSSTable(option *Option) *SSTable {
	mf, err := OpenMmapFileWithName(option.FileName, os.O_CREATE|os.O_RDWR, option.MaxSz)
	if err != nil {
		return nil
	}
	return &SSTable{mf: mf, fid: option.FID}
}

func (s *SSTable) Init() error {
	blockMeta, err := s.initSSTable()
	if err != nil {
		return err
	}
	s.minKey = blockMeta.GetKey()
	blocks := len(s.tableIdx.GetOffsets())
	s.maxKey = s.tableIdx.GetOffsets()[blocks-1].Key
	return nil
}

func (s *SSTable) read(offset, size int) ([]byte, error) {
	// read from mmap in-memory
	if len(s.mf.Data) > 0 {
		if len(s.mf.Data[offset:]) < size {
			return nil, io.EOF
		}
		return s.mf.Data[offset : offset+size], nil
	}
	// read by disk io
	buf := make([]byte, size)
	_, err := s.mf.Fd.ReadAt(buf, int64(offset))
	return buf, err
}

func (s *SSTable) initSSTable() (*pb.BlockOffset, error) {
	off := len(s.mf.Data)
	// last 4 bit : checksum_len
	off -= 4
	buf, err := s.read(off, 4)
	if err != nil {
		return nil, err
	}
	checksumLen := int(binary.BigEndian.Uint32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less zero")
	}
	// find checksum
	off -= checksumLen
	expectChecksum, err := s.read(off, checksumLen)
	if err != nil {
		return nil, err
	}
	// index len : 4 bit
	off -= 4
	buf, err = s.read(off, 4)
	if err != nil {
		return nil, err
	}
	s.idxLen = int(binary.BigEndian.Uint32(buf))
	// read index
	off -= s.idxLen
	s.idxStart = off
	buf, err = s.read(off, s.idxLen)
	if err != nil {
		return nil, err
	}
	if err := utils.VerifyChecksum(buf, expectChecksum); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", s.mf.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(buf, indexTable); err != nil {
		return nil, err
	}
	s.tableIdx = indexTable
	if len(s.tableIdx.BloomFilter) > 0 {
		s.hasBloomFilter = true
	}
	if len(s.tableIdx.GetOffsets()) > 0 {
		return s.tableIdx.GetOffsets()[0], nil
	}
	return nil, errors.New("offset is nil")
}
