package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}

// FID 根据file name 获取其fid
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0
	}
	return uint64(id)
}

func SSTFullFileName(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", fid))
}

func LoadIDMap(dir string) map[uint64]struct{} {
	ret := make(map[uint64]struct{})
	finfo, err := ioutil.ReadDir(dir)
	if err != nil {
		return ret
	}
	for _, info := range finfo {
		if info.IsDir() {
			continue
		}
		if fid := FID(info.Name()); fid != 0 {
			ret[fid] = struct{}{}
		}
	}
	return ret
}

func CompareKeys(key1, key2 []byte) int {
	if len(key1) <= 8 || len(key2) <= 8 {
		panic("key length shouldn't less than 8")
	}
	// compare true val first
	if com := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); com != 0 {
		return com
	}
	// compare timestamp then
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

func Copy(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}
