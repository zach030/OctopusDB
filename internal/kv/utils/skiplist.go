package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header   *Element
	rand     *rand.Rand
	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			entry:  nil,
			score:  0,
		},
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel: defaultMaxLevel,
		length:   0,
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
	max := len(s.header.levels)
	prev := s.header
	var prevElements [defaultMaxLevel]*Element
	for i := max - 1; i >= 0; {
		// 从最上层开始查，纵向遍历
		prevElements[i] = prev
		// 横向遍历
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					// 如果相等，直接插入
					ele = next
					ele.entry = data
					s.size += ele.Entry().Size()
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
	ele = newElement(level, score, data)
	for i := 0; i < level; i++ {
		// 逐层单链表插入节点
		ele.levels[i] = prevElements[i].levels[i]
		prevElements[i].levels[i] = ele
	}
	s.size += data.Size()
	s.length++
	return nil
}

// a(1)--------------------e(5)--------------------i(9)
// a(1)--------c(3)--------e(5)--------g(7)--------i(9)
// a(1)--b(2)--c(3)--d(4)--e(5)--f(6)--g(7)--h(8)--i(9)

// Search 先横向遍历，再向下
func (s *SkipList) Search(key []byte) *Entry {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.length == 0 {
		return nil
	}
	prev := s.header
	i := len(s.header.levels) - 1
	score := calcScore(key)
	for i >= 0 {
		// 从最顶层查找
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := compare(score, key, next); comp <= 0 {
				if comp == 0 {
					// 找到
					return next.entry
				}
				// 下一个比要找的大，说明这层已经找不到，i--进入下一层找
				break
			}
			prev = next
		}
		// 进入下一层
		i--
	}
	return nil
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

type SkipListIterator struct {
	ele *Element
	sl  *SkipList
}

func (s *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{
		ele: s.header,
		sl:  s,
	}
}

func (s *SkipListIterator) Next() {
	s.ele = s.ele.levels[0]
}

func (s *SkipListIterator) Rewind() {
	s.ele = s.sl.header.levels[0]
}

func (s *SkipListIterator) Valid() bool {
	return s.sl != nil
}

func (s *SkipListIterator) Close() error {
	return nil
}

func (s *SkipListIterator) Seek(bytes []byte) {
}

func (s *SkipListIterator) Item() Item {
	//vo, vs := decodeValue(s.ele.)
	// todo add arena for skip-list
	return &Entry{}
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
	levels []*Element
	entry  *Entry
	score  float64
	value  uint64 //将value的off和size组装成一个uint64，实现原子化的操作
}

func newElement(level int, score float64, entry *Entry) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (e *Element) Entry() *Entry {
	return e.entry
}

func compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}
