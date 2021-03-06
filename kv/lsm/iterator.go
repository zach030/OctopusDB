package lsm

import (
	"bytes"
	"fmt"
	"sort"

	utils2 "github.com/zach030/OctopusDB/kv/utils"
)

//Iterator  通用的Iterator迭代器
type Iterator struct {
	it    Item
	iters []utils2.Iterator // memtable, immemtable, level-manager
}

type Item struct {
	e *utils2.Entry
}

func (it *Item) Entry() *utils2.Entry {
	return it.e
}

// NewIterator 创建LSM迭代器
func (l *LSM) NewIterator(opt *utils2.Options) []utils2.Iterator {
	iter := &Iterator{}
	iter.iters = make([]utils2.Iterator, 0)
	iter.iters = append(iter.iters, l.memTable.NewIterator(opt))
	for _, imm := range l.imMemTable {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, l.levels.NewIterator(opt))
	return iter.iters
}

func (iter *Iterator) Next() {
	iter.iters[0].Next()
}

func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}

func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}

func (iter *Iterator) Item() utils2.Item {
	return iter.iters[0].Item()
}

func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}

// memIterator 内存表迭代器
type memIterator struct {
	innerIter utils2.Iterator // 内存表中的迭代器是跳表skiplist
}

func (m *MemTable) NewIterator(opt *utils2.Options) utils2.Iterator {
	return &memIterator{innerIter: m.skipList.NewSkipListIterator()}
}

func (iter *memIterator) Next() {
	iter.innerIter.Next()
}

func (iter *memIterator) Valid() bool {
	return iter.innerIter.Valid()
}

func (iter *memIterator) Rewind() {
	iter.innerIter.Rewind()
}

func (iter *memIterator) Item() utils2.Item {
	return iter.innerIter.Item()
}

func (iter *memIterator) Close() error {
	return iter.innerIter.Close()
}

func (iter *memIterator) Seek(key []byte) {
}

// levelManager上的迭代器
type levelIterator struct {
	it    *utils2.Item
	iters []*Iterator
}

func (l *LevelManager) NewIterator(options *utils2.Options) utils2.Iterator {
	return &levelIterator{}
}
func (iter *levelIterator) Next() {
}
func (iter *levelIterator) Valid() bool {
	return false
}
func (iter *levelIterator) Rewind() {

}
func (iter *levelIterator) Item() utils2.Item {
	return &Item{}
}
func (iter *levelIterator) Close() error {
	return nil
}

func (iter *levelIterator) Seek(key []byte) {
}

// ConcatIterator 将table 数组链接成一个迭代器，这样迭代效率更高
type ConcatIterator struct {
	idx     int // Which iterator is active now.
	cur     utils2.Iterator
	iters   []utils2.Iterator // Corresponds to tables.
	tables  []*table          // Disregarding reversed, this is in ascending order.
	options *utils2.Options   // Valid options are REVERSED and NOCACHE.
}

// NewConcatIterator creates a new concatenated iterator
func NewConcatIterator(tbls []*table, opt *utils2.Options) *ConcatIterator {
	iters := make([]utils2.Iterator, len(tbls))
	return &ConcatIterator{
		options: opt,
		iters:   iters,
		tables:  tbls,
		idx:     -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if !s.options.IsAsc {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			// End of list. Valid will become false.
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.options.IsAsc {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}

func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

func (s *ConcatIterator) Close() error {
	for _, it := range s.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return fmt.Errorf("ConcatIterator:%+v", err)
		}
	}
	return nil
}

func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if s.options.IsAsc {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return utils2.CompareKeys(s.tables[i].sst.MaxKey(), key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return utils2.CompareKeys(s.tables[n-1-i].sst.MinKey(), key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.setIdx(idx)
	s.cur.Seek(key)
}

func (s *ConcatIterator) Item() utils2.Item {
	return s.cur.Item()
}

func (s *ConcatIterator) setIdx(idx int) {
	s.idx = idx
	// 判断idx的范围
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
		return
	}
	// 如果不存在，则new iterator
	if s.iters[idx] == nil {
		s.iters[idx] = s.tables[idx].NewIterator(s.options)
	}
	s.cur = s.iters[s.idx]
}

// MergeIterator 多路合并迭代器
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	left  node
	right node
	small *node

	curKey  []byte
	reverse bool
}

type node struct {
	valid bool
	entry *utils2.Entry
	iter  utils2.Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (n *node) setIterator(iter utils2.Iterator) {
	n.iter = iter
	// It's okay if the type assertion below fails and n.merge/n.concat are set to nil.
	// We handle the nil values of merge and concat in all the methods.
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

func (n *node) setKey() {
	switch {
	case n.merge != nil:
		n.valid = n.merge.small.valid
		if n.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.entry = n.concat.Item().Entry()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.entry = n.iter.Item().Entry()
		}
	}
}

func (n *node) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

func (mi *MergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}
	cmp := utils2.CompareKeys(mi.small.entry.Key, mi.bigger().entry.Key)
	switch {
	case cmp == 0: // Both the keys are equal.
		// In case of same keys, move the right iterator ahead.
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0: // Small is less than bigger().
		if mi.reverse {
			mi.swapSmall()
		} else {
			// we don't need to do anything. Small already points to the smallest.
		}
		return
	default: // bigger() is less than small.
		if mi.reverse {
			// Do nothing since we're iterating in reverse. Small currently points to
			// the bigger key and that's okay in reverse iteration.
		} else {
			mi.swapSmall()
		}
		return
	}
}

func (mi *MergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.entry.Key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

func (mi *MergeIterator) setCurrent() {
	if mi.small.entry == nil && mi.small.valid == true {
		panic(fmt.Errorf("mi.small.entry is nil"))
	}
	if mi.small.valid {
		mi.curKey = append(mi.curKey[:0], mi.small.entry.Key...)
	}
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

// Seek brings us to element with key >= given key.
func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}

// Key returns the key associated with the current iterator.
func (mi *MergeIterator) Item() utils2.Item {
	return mi.small.iter.Item()
}

// Close implements Iterator.
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return fmt.Errorf("MergeIterator:%v", err1)
	}
	return fmt.Errorf("MergeIterator:%v", err2)
}

// NewMergeIterator creates a merge iterator.
func NewMergeIterator(iters []utils2.Iterator, reverse bool) utils2.Iterator {
	switch len(iters) {
	case 0:
		return &Iterator{}
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// Assign left iterator randomly. This will be fixed when user calls rewind/seek.
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]utils2.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
