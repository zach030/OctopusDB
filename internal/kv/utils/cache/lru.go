package cache

import "container/list"

type windowLRU struct {
	cap  int
	data map[uint64]*list.Element
	list *list.List
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func NewWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		cap:  size,
		data: data,
		list: list.New(),
	}
}

// add 向lru缓存中添加对象
func (w *windowLRU) add(item storeItem) (storeItem, error) {
	// 如果缓存没满，直接加到头部，不淘汰
	if w.list.Len() < w.cap {
		w.data[item.key] = w.list.PushFront(&item)
		return storeItem{}, nil
	}
	// 选出末尾的元素
	last := w.list.Back()
	oldItem := last.Value.(*storeItem)
	// 在map中删除映射
	delete(w.data, oldItem.key)
	var exitItem storeItem
	exitItem, *oldItem = *oldItem, item

	w.data[item.key] = last
	w.list.MoveToFront(last)
	return exitItem, nil
}

func (w *windowLRU) get(e *list.Element) {
	w.list.MoveToFront(e)
}

// segmentLRU 分段lru，写入的时候都写入seg-one，读取的时候如果此元素已处于seg-two直接返回，如果是在seg-one且seg-two还没有满
// 则从seg-one中移除，并加入seg-two，如果seg-two已满，则需要将seg-two末尾的元素淘汰，放到seg-one中，将当前查询的元素写入seg-two的末尾
type segmentLRU struct {
	data                 map[uint64]*list.Element
	segOneCap, segTwoCap int
	segOne, segTwo       *list.List
}

const (
	STAGE_ONE = iota
	STAGE_TWO
)

func newSegLRU(data map[uint64]*list.Element, oneCap, twoCap int) *segmentLRU {
	return &segmentLRU{
		data:      data,
		segOneCap: oneCap,
		segTwoCap: twoCap,
		segOne:    list.New(),
		segTwo:    list.New(),
	}
}

func (s *segmentLRU) add(item storeItem) {
	item.stage = 1
	// 如果空间足够，则放入map中
	if s.segOne.Len() < s.segOneCap || s.Len() < s.segOneCap+s.segTwoCap {
		s.data[item.key] = s.segOne.PushFront(&item)
		return
	}
	old := s.segOne.Back()
	oldItem := old.Value.(*storeItem)
	delete(s.data, oldItem.key)
	oldItem = &item
	s.data[item.key] = old
	s.segOne.MoveToFront(old)
}

func (s *segmentLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)
	// 1. item is in stage two
	if item.stage == STAGE_TWO {
		s.segTwo.MoveToFront(v)
		return
	}
	// 2. item is in stage one
	// 2.1 stage two list has available space
	if s.segTwo.Len() < s.segTwoCap {
		// remove from stage one
		s.segOne.Remove(v)
		item.stage = STAGE_TWO
		s.data[item.key] = s.segTwo.PushFront(v)
		return
	}
	// 2.2 stage two list is full
	last := s.segTwo.Back()
	lastItem := last.Value.(*storeItem)
	// move the back element in seg-two list to seg-one
	// add new element to seg two
	*lastItem, *item = *item, *lastItem

	// lastItem refer to newItem now, will add to seg-two
	// item refer to oldItem from removed seg-two, will add to seg-one
	lastItem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	s.data[lastItem.key] = last
	s.data[item.key] = v

	s.segOne.MoveToFront(v)
	s.segTwo.MoveToFront(last)
}

func (s *segmentLRU) Len() int {
	return s.segOne.Len() + s.segTwo.Len()
}

func (s *segmentLRU) victim() *storeItem {
	if s.Len() < s.segOneCap+s.segTwoCap {
		return nil
	}

	v := s.segOne.Back()
	return v.Value.(*storeItem)
}
