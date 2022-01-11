package file

import (
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

type Manifest struct {
	Levels []LevelManifest          // 每个level的table集合
	Tables map[uint64]TableManifest // tableID--{level,crc}的映射
}

type (
	LevelManifest struct {
		Tables map[uint64]struct{}
	}
	TableManifest struct {
		Level    uint8
		CheckSum []byte
	}
	TableMeta struct {
		ID       uint64
		CheckSum []byte
	}
)

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
		RewriteManifest(opt.Dir, m)
		return nil, err
	}

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
func RewriteManifest(dir string, m *Manifest) error {
	f, err := os.OpenFile(filepath.Join(dir, utils.ReManifestFileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	modifies := pb.ManifestModifies{Modifies: m.GetModifies()}
	// todo 覆盖写
	return nil
}
