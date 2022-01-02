package utils

import (
	"sync/atomic"
	"unsafe"
)

const (
	NodeSize  = int(unsafe.Sizeof(Element{}))
	eachSize  = int(unsafe.Sizeof(uint32(0)))
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

type Arena struct {
	offset uint32
	buf    []byte
}

func newArena(n int) *Arena {
	return &Arena{
		offset: 1,
		buf:    make([]byte, n),
	}
}

// allocate 分配空间
func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.offset, sz)
	if len(s.buf)-int(offset) < NodeSize {
		grow := uint32(len(s.buf))
		if grow > 1<<30 {
			grow = 1 << 30
		}
		if grow < sz {
			grow = sz
		}
		newBuf := make([]byte, len(s.buf)+int(grow))
		copy(newBuf, s.buf)
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) putNode(height int) uint32 {
	// 分配32位无符号整数需要的空间
	sz := (defaultMaxLevel - height) * eachSize
	l := uint32(NodeSize - sz + nodeAlign)
	n := s.allocate(l)
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}
