package file

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/zach030/OctopusDB/internal/kv/pb"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type ManifestFile struct {
	f        *os.File
	option   *Option
	manifest *Manifest
}

// Manifest 存放当前kv的level元数据
// 磁盘存储结构： magic-num | version | length change | crc   | change
//                 32 bit |  32bit  |   32 bit      | 32bit | ...
type Manifest struct {
	Levels []LevelManifest          // 每个level的table集合
	Tables map[uint64]TableManifest // tableID--{level,crc}的映射
}

type (
	LevelManifest struct {
		Tables map[uint64]struct{} // 每层的table集合
	}
	TableManifest struct { // 每个table的元数据
		Level    uint8  // 所在level
		CheckSum []byte // 校验和
	}
	TableMeta struct {
		ID       uint64
		CheckSum []byte
	}
)

// OpenManifestFile 打开manifest文件
func OpenManifestFile(opt *Option) (*ManifestFile, error) {
	mf := &ManifestFile{option: opt}
	path := filepath.Join(opt.Dir, utils.ManifestFileName)
	// 打开路径下的manifest文件
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			// 如果文件存在，有其他错误，返回
			return mf, err
		}
		// 如果文件不存在，则新写manifest
		m := newManifest()
		// 覆盖写manifest文件
		fp, err1 := RewriteManifest(opt.Dir, m)
		if err1 != nil {
			return nil, err1
		}
		mf.f = fp
		mf.manifest = m
		return nil, err
	}
	// 如果manifest文件之前存在，需要重放记录sst文件层级信息
	ma, err := ReplayManifest(f)
	if err != nil {

	}
	mf.manifest = ma
	return mf, nil
}

func newManifest() *Manifest {
	return &Manifest{
		Levels: make([]LevelManifest, 0),
		Tables: make(map[uint64]TableManifest),
	}
}

func (m *Manifest) GetModifies() []*pb.ManifestModify {
	changes := make([]*pb.ManifestModify, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateModify(id, int(tm.Level), tm.CheckSum))
	}
	return changes
}

func newCreateModify(id uint64, level int, checksum []byte) *pb.ManifestModify {
	return &pb.ManifestModify{
		Id:       id,
		Op:       pb.ManifestModify_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// RewriteManifest 覆盖写manifest文件
func RewriteManifest(dir string, m *Manifest) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, utils.ReManifestFileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 8)
	// magic-num | version
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], utils.MagicVersion)
	modifies := pb.ManifestModifies{Modifies: m.GetModifies()}
	modifyBuf, err := modifies.Marshal()
	if err != nil {
		f.Close()
		return nil, err
	}
	// length of modify | crc
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(modifyBuf)))
	binary.BigEndian.PutUint32(buf[12:16], crc32.Checksum(modifyBuf, utils.CastagnoliCrcTable))
	buf = append(buf, modifyBuf...)
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return nil, err
	}
	// 同步写磁盘
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	// 关闭文件，进行rename
	if err = f.Close(); err != nil {
		return nil, err
	}
	if err = os.Rename(filepath.Join(dir, utils.ReManifestFileName), filepath.Join(dir, utils.ManifestFileName)); err != nil {
		return nil, err
	}
	fp, err := os.OpenFile(filepath.Join(dir, utils.ManifestFileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fp.Close()
		return nil, err
	}
	if _, err = fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, err
	}
	if err = fp.Sync(); err != nil {
		fp.Close()
		return nil, err
	}
	return fp, nil
}

func ReplayManifest(f *os.File) (*Manifest, error) {
	return nil, nil
}
