package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

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
	Levels    []LevelManifest          // 每个level的table集合
	Tables    map[uint64]TableManifest // tableID--{level,crc}的映射
	Creations int
	Deletions int
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
		return mf, nil
	}
	// 如果manifest文件之前存在，需要重放记录sst文件层级信息
	ma, offset, err := ReplayManifest(f)
	if err != nil {
		_ = f.Close()
		return mf, err
	}
	// Truncate file so we don't have a half-written entry at the end.
	if err := f.Truncate(offset); err != nil {
		_ = f.Close()
		return mf, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return mf, err
	}
	mf.f = f
	mf.manifest = ma
	return mf, nil
}

func newManifest() *Manifest {
	return &Manifest{
		Levels: make([]LevelManifest, 0),
		Tables: make(map[uint64]TableManifest),
	}
}

func (f *ManifestFile) Manifest() *Manifest {
	return f.manifest
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
	var crcBuf [8]byte
	// length of modify | crc
	binary.BigEndian.PutUint32(crcBuf[0:4], uint32(len(modifyBuf)))
	binary.BigEndian.PutUint32(crcBuf[4:8], crc32.Checksum(modifyBuf, utils.CastagnoliCrcTable))
	buf = append(buf, crcBuf[:]...)
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

func ReplayManifest(f *os.File) (*Manifest, int64, error) {
	reader := bufio.NewReader(f)
	var magicBuf [8]byte
	if _, err := reader.Read(magicBuf[:]); err != nil {
		return &Manifest{}, 0, err
	}
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, errors.New("incorrect magic version")
	}
	if v := binary.BigEndian.Uint32(magicBuf[4:8]); v != utils.MagicVersion {
		return &Manifest{}, 0, errors.New("incorrect version")
	}
	var offset = int64(8)
	m := newManifest()
	for {
		var lenCrcBuf [8]byte
		if _, err := reader.Read(lenCrcBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		offset += int64(len(lenCrcBuf))
		lengthOfChange := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		crc := binary.BigEndian.Uint32(lenCrcBuf[4:8])
		change := make([]byte, lengthOfChange)
		if _, err := reader.Read(change); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		offset += int64(lengthOfChange)
		if crc32.Checksum(change, utils.CastagnoliCrcTable) != crc {
			return &Manifest{}, 0, errors.New("invalid crc")
		}
		var changes pb.ManifestModifies
		if err := changes.Unmarshal(change); err != nil {
			return &Manifest{}, 0, err
		}
		if err := m.applyModifies(changes); err != nil {
			return &Manifest{}, 0, err
		}
	}
	return m, offset, nil
}

// FilterValidTables 用来将磁盘中存储的sst文件集合与内存中的manifest进行对比过滤
// 两个集合：manifest，sst目录下集合
// 以manifest为基准，保证manifest中的所有文件都存在：如果出现某table存在于manifest中，但是磁盘中并不存在此sst文件，返回error
// 如果manifest中多了，需要将多余的文件删除
func (f *ManifestFile) FilterValidTables(ids map[uint64]struct{}) error {
	for id := range f.manifest.Tables {
		if _, ok := ids[id]; !ok {
			return fmt.Errorf("file not exist for table:%v", id)
		}
	}
	for id := range ids {
		if _, ok := f.manifest.Tables[id]; !ok {
			// need to delete
			fname := utils.SSTFullFileName(f.option.Dir, id)
			if err := os.Remove(fname); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manifest) applyModifies(modifies pb.ManifestModifies) error {
	for _, modify := range modifies.Modifies {
		if err := m.applyModify(modify); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manifest) applyModify(modify *pb.ManifestModify) error {
	switch modify.Op {
	case pb.ManifestModify_CREATE:
		if _, ok := m.Tables[modify.Id]; !ok {
			return errors.Errorf("not found new table")
		}
		m.Tables[modify.Id] = TableManifest{
			Level:    uint8(modify.Level),
			CheckSum: modify.Checksum,
		}
		for len(m.Levels) <= int(modify.Level) {
			m.Levels = append(m.Levels, LevelManifest{Tables: make(map[uint64]struct{})})
		}
		m.Levels[modify.Level].Tables[modify.Id] = struct{}{}
		m.Creations++
	case pb.ManifestModify_DELETE:
		if _, ok := m.Tables[modify.Id]; !ok {
			return errors.Errorf("not found table")
		}
		delete(m.Tables, modify.Id)
		delete(m.Levels[modify.Level].Tables, modify.Id)
		m.Deletions++
	default:
		return errors.Errorf("invalid modify type")
	}
	return nil
}

func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestModify {
	return &pb.ManifestModify{
		Id:       id,
		Op:       pb.ManifestModify_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

func (f *ManifestFile) AddChanges(changesParam []*pb.ManifestModify) error {
	return f.addChanges(changesParam)
}

func (f *ManifestFile) addChanges(changesParam []*pb.ManifestModify) error {
	// todo add changes to manifest
	return nil
}

// AddTableMeta 存储level表到manifest的level中
func (f *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	err = f.addChanges([]*pb.ManifestModify{
		newCreateChange(t.ID, levelNum, t.CheckSum),
	})
	return err
}
