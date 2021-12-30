package lsm

import (
	"math/rand"
	"sync"
	"time"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element
	rand *rand.Rand
	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList()*SkipList{
	return &SkipList{
		header:   &Element{
			levels: make([]*Element,defaultMaxLevel),
			entry:  nil,
			score:  0,
		},
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel: defaultMaxLevel,
		length:   0,
	}
}

func (s *SkipList) Add(data *utils.Entry) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	//score := s.calcScore(data.Key)
	//var ele *Element
	//max := len(s.header.levels)
	//prev := s.header

	//todo implement add
	return nil
}

func (s *SkipList) Search(key []byte) *utils.Entry {
	// todo implement search
	return nil
}

func (s *SkipList) calcScore(key []byte) (score float64) {
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

type SkipListIterator struct {
	ele *Element
	sl *SkipList
}

func (s *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{
		ele: s.header,
		sl:  s,
	}
}

func (s *SkipListIterator) Next() {

}

func (s *SkipListIterator) Rewind() {

}

func (s *SkipListIterator) Valid() bool {
	return s.sl!=nil
}

func (s *SkipListIterator) Close() error {
	return nil
}

func (s *SkipListIterator) Seek(bytes []byte) {
}

type Element struct {
	levels []*Element
	entry *utils.Entry
	score float64
}

func newElement(level int,score float64,entry *utils.Entry)*Element{
	return &Element{
		levels: make([]*Element,level),
		entry:  entry,
		score:  score,
	}
}