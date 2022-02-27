package cache

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	i1 = storeItem{key: 1, value: "val1"}
	i2 = storeItem{key: 2, value: "val2"}
	i3 = storeItem{key: 3, value: "val3"}
	i4 = storeItem{key: 4, value: "val4"}
	i5 = storeItem{key: 5, value: "val5"}

	res storeItem
	err error
)

func Test_windowLRU_add(t *testing.T) {
	var lru = NewWindowLRU(3, make(map[uint64]*list.Element))
	res, err = lru.add(i1)
	assert.Equal(t, uint64(0), res.key)
	printList("1", lru.list)
	res, err = lru.add(i2)
	assert.Equal(t, uint64(0), res.key)
	printList("1", lru.list)
	res, err = lru.add(i3)
	assert.Equal(t, uint64(0), res.key)
	printList("1", lru.list)
	res, err = lru.add(i4)
	assert.Equal(t, uint64(1), res.key)
	printList("1", lru.list)
	res, err = lru.add(i5)
	assert.Equal(t, uint64(2), res.key)
	printList("1", lru.list)
}

func Test_segmentLRU_add(t *testing.T) {
	var slru = newSegLRU(make(map[uint64]*list.Element), 2, 2)
	t.Run("add", func(t *testing.T) {
		slru.add(i1)
		assert.Equal(t, STAGE_ONE, i1.stage)
		slru.add(i2)
		assert.Equal(t, STAGE_ONE, i2.stage)
		slru.add(i3)
		assert.Equal(t, STAGE_ONE, i3.stage)
		slru.add(i4)
		assert.Equal(t, STAGE_ONE, i4.stage)
		slru.add(i5)
		assert.Equal(t, STAGE_ONE, i5.stage)
		// list-1: 5-->4-->3-->2-->1
		// list-2: nil
		printList("1", slru.segOne)
	})

	back := slru.segOne.Back()
	slru.get(back)
	assert.Equal(t, STAGE_TWO, back.Value.(*storeItem).stage)
	// list-1: 5-->4-->3-->2
	// list-2: 1
	printList("1", slru.segOne)
	printList("2", slru.segTwo)

	back = slru.segOne.Back()
	slru.get(back)
	assert.Equal(t, STAGE_TWO, back.Value.(*storeItem).stage)
	// list-1: 5-->4-->3
	// list-2: 2-->1
	printList("1", slru.segOne)
	printList("2", slru.segTwo)

	back = slru.segOne.Back()
	slru.get(back)
	assert.Equal(t, STAGE_TWO, back.Value.(*storeItem).stage)
	// list-1: 1-->5-->4
	// list-2: 3-->2-->1
	printList("1", slru.segOne)
	printList("2", slru.segTwo)
}

func printList(name string, l *list.List) {
	fmt.Printf("list-%s:", name)
	for i := l.Front(); i != nil; i = i.Next() {
		fmt.Printf("%v ", i.Value.(*storeItem).value)
	}
	fmt.Println()
}
