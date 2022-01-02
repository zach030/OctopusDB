package file

import (
	"fmt"
	"os"
)

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

func OpenMmapFileWithName(filename string, flag int, maxSz int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, fmt.Errorf("unable to open mmap file:%v,err:%v", filename, err)
	}
	write := true
	if flag == os.O_RDONLY {
		write = false
	}
	return OpenMmapFile(fd, maxSz, write)
}

func OpenMmapFile(fd *os.File, size int, write bool) (*MmapFile, error) {
	//fname := fd.Name()
	//fstat, err := fd.Stat()
	//if err != nil {
	//	return nil, err
	//}
	//
	return nil, nil
}

func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	return nil
}
