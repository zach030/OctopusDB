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

func newArena(n int64) *Arena {
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
		if len(s.buf) != copy(newBuf, s.buf) {
			panic("assert buffer grow panic")
		}
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
func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}
	return (*Element)(unsafe.Pointer(&s.buf[offset]))
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
func (s *Arena) getElementOffset(e *Element) uint32 {
	if e == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(e)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&e.levels[h])
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.offset))
}
