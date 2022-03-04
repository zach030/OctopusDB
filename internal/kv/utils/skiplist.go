package utils

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	maxLevel   int
	currHeight int32
	lock       sync.RWMutex
	headOffset uint32 //头结点在arena当中的偏移量
	arena      *Arena
}

func NewSkipList(arenaSize int64) *SkipList {
	// 初始化内存管理组件
	arena := newArena(arenaSize)
	// 初始化头节点
	head := newElement(arena, nil, ValueStruct{}, defaultMaxLevel)
	// 获取头节点的偏移量
	ho := arena.getElementOffset(head)
	return &SkipList{
		arena:      arena,
		currHeight: 1,
		headOffset: ho,
	}
}

// a(1)--------------------e(5)--------------------i(9)
// a(1)--------c(3)--------e(5)--------g(7)--------i(9)
// a(1)--b(2)--c(3)--d(4)--e(5)--f(6)--g(7)--h(8)--i(9)

// Add 跳表的插入，目的是找到level=0的前置指针，再randLevel，构建多层
func (s *SkipList) Add(data *Entry) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	score := calcScore(data.Key)
	var ele *Element
	value := ValueStruct{Value: data.Value}
	// max 从最高处开始
	max := s.currHeight
	// 从内存块中获取头节点
	prev := s.arena.getElement(s.headOffset)
	var prevElements [defaultMaxLevel]*Element
	for i := max - 1; i >= 0; {
		// 从最上层开始查，纵向遍历
		prevElements[i] = prev
		// 横向遍历
		for next := s.getNext(prev, int(i)); next != nil; next = s.getNext(prev, int(i)) {
			if comp := s.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					// 如果相等，直接插入
					vo := s.arena.putVal(value)
					encV := encodeValue(vo, value.EncodedSize())
					next.value = encV
					return nil
				}
				// 如果next的key更大，则找到了待插入点
				break
			}
			// 像单链表一样，prev移动到next
			prev = next
			prevElements[i] = prev
		}
		// 找到比待插入key大的next节点，因此要在prev之后插入节点
		topLast := prev.levels[i]
		// 向下跳，找到prev与last之间还有元素的level
		for ; i >= 0 && prev.levels[i] == topLast; i-- {
			prevElements[i] = prev
		}
	}
	// 找到了待插入的prev，构造level，开始逐层插入
	level := s.randLevel()
	ele = newElement(s.arena, data.Key, ValueStruct{Value: data.Value}, level)
	off := s.arena.getElementOffset(ele)
	for i := 0; i < level; i++ {
		// 逐层单链表插入节点
		ele.levels[i] = prevElements[i].levels[i]
		prevElements[i].levels[i] = off
	}
	return nil
}

// a(1)--------------------e(5)--------------------i(9)
// a(1)--------c(3)--------e(5)--------g(7)--------i(9)
// a(1)--b(2)--c(3)--d(4)--e(5)--f(6)--g(7)--h(8)--i(9)

// Search 先横向遍历，再向下
func (s *SkipList) Search(key []byte) *Entry {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.arena.Size() == 0 {
		return nil
	}
	prev := s.arena.getElement(s.headOffset)
	i := s.currHeight
	score := calcScore(key)
	for i >= 0 {
		// 从最顶层查找
		for next := s.getNext(prev, int(i)); next != nil; next = s.getNext(prev, int(i)) {
			if comp := s.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					// 找到
					vo, vSize := decodeValue(next.value)
					return &Entry{Key: key, Value: s.arena.getVal(vo, vSize).Value}
				}
				// 下一个比要找的大，说明这层已经找不到，i--进入下一层找
				break
			}
			prev = next
		}
		// 进入下一层
		topLevel := prev.levels[i]

		for i--; i >= 0 && prev.levels[i] == topLevel; i-- {

		}
	}
	return nil
}

func (s *SkipList) getNext(e *Element, height int) *Element {
	return s.arena.getElement(e.getNextOffset(height))
}

func calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)
	if l > 8 {
		l = 8
	}
	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}
	score = float64(hash)
	return
}

func (s *SkipList) randLevel() int {
	if s.maxLevel <= 1 {
		return 1
	}
	i := 1
	for ; i < s.maxLevel; i++ {
		if rand.Intn(1000)%2 == 0 {
			return i
		}
	}
	return i
}

func (s *SkipList) Close() error {
	return nil
}

type SkipListIterator struct {
	ele *Element  // 当前所持有的元素
	sl  *SkipList // 跳表
}

func (s *SkipList) NewIterator() Iterator {
	return &SkipListIterator{
		sl: s,
	}
}

func (s *SkipListIterator) Next() {
	if !s.Valid() {
		panic("skiplist iterator element is nil")
	}
	s.ele = s.sl.getNext(s.ele, 0)
}

func (s *SkipListIterator) Rewind() {
	// 从arena中拿出下一个元素
	head := s.sl.arena.getElement(s.sl.headOffset)
	// 赋给ele
	s.ele = s.sl.getNext(head, 0)
}

func (s *SkipListIterator) Valid() bool {
	return s.ele != nil
}

func (s *SkipListIterator) Close() error {
	return nil
}

func (s *SkipListIterator) Seek(bytes []byte) {
}

func (s *SkipListIterator) Item() Item {
	vo, vs := decodeValue(s.ele.value)
	return &Entry{
		Key:       s.sl.arena.getKey(s.ele.keyOffset, s.ele.keySize),
		Value:     s.sl.arena.getVal(vo, vs).Value,
		ExpiresAt: s.sl.arena.getVal(vo, vs).ExpiresAt,
	}
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

type Element struct {
	levels [defaultMaxLevel]uint32
	//levels    []*Element
	entry     *Entry
	keyOffset uint32
	keySize   uint16
	height    uint16
	score     float64 // 哈希值，加速比较
	value     uint64  //将value的off和size组装成一个uint64，实现原子化的操作
}

func (e *Element) key(arena *Arena) []byte {
	return arena.getKey(e.keyOffset, e.keySize)
}

func newElement(arena *Arena, key []byte, v ValueStruct, height int) *Element {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())
	elem := arena.getElement(nodeOffset) //这里的elem是根据内存中的地址来读取的，不是arena中的offset
	elem.score = calcScore(key)
	elem.keyOffset = keyOffset
	elem.keySize = uint16(len(key))
	elem.height = uint16(height)
	elem.value = val
	return elem
}

func (e *Element) Entry() *Entry {
	return e.entry
}

func (s *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.key(s.arena))
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}
