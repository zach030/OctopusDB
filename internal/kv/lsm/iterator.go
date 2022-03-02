package lsm

import "github.com/zach030/OctopusDB/internal/kv/utils"

type Iterator struct {
	it    Item
	iters []utils.Iterator
}
type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}
