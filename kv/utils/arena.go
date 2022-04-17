package utils

import (
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
)

const (
	NodeSize  = int(unsafe.Sizeof(node{}))
	eachSize  = int(unsafe.Sizeof(uint32(0)))
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

// Arena 内存管理模块，分配一整块内存buf，对应memetable，当此buf分配完时对应memtable-->immemtable的转换
type Arena struct {
	offset     uint32
	buf        []byte
	shouldGrow bool
}

func newArena(n int64) *Arena {
	return &Arena{
		offset: 1,
		buf:    make([]byte, n),
	}
}

// allocate 分配空间
func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.offset, sz)
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}
	if len(s.buf)-int(offset) < NodeSize {
		grow := uint32(len(s.buf))
		if grow > 1<<30 {
			grow = 1 << 30
		}
		if grow < sz {
			grow = sz
		}
		newBuf := make([]byte, len(s.buf)+int(grow))
		if len(s.buf) != copy(newBuf, s.buf) {
			panic("assert buffer grow panic")
		}
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) putNode(height int) uint32 {
	// 分配32位无符号整数需要的空间
	sz := (maxHeight - height) * eachSize
	l := uint32(NodeSize - sz + nodeAlign)
	n := s.allocate(l)
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// putKey put key into arena
func (s *Arena) putKey(key []byte) uint32 {
	size := uint32(len(key))
	offset := s.allocate(size)
	buf := s.buf[offset : offset+size]
	if len(key) != copy(buf, key) {
		panic("assert buffer copy panic")
	}
	return offset
}

// putVal get encoded size of value-struct, allocate in arena and put val in buf
func (s *Arena) putVal(val ValueStruct) uint32 {
	size := val.EncodedSize()
	offset := s.allocate(size)
	val.EncodeValue(s.buf[offset:])
	return offset
}

// getElement get element by given address
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) ValueStruct {
	val := s.buf[offset : offset+size]
	var vs ValueStruct
	vs.DecodeValue(val)
	return vs
}

// getElementOffset 获取在arena中的偏移量
func (s *Arena) getNodeOffset(e *node) uint32 {
	if e == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(e)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.offset))
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
