package kv

import (
	"github.com/zach030/OctopusDB/kv/lsm"
	utils2 "github.com/zach030/OctopusDB/kv/utils"
)

type DBIterator struct {
	iitr utils2.Iterator
	vlog *valueLog
}
type Item struct {
	e *utils2.Entry
}

func (it *Item) Entry() *utils2.Entry {
	return it.e
}
func (o *OctopusDB) NewIterator(opt *utils2.Options) utils2.Iterator {
	iters := make([]utils2.Iterator, 0)
	iters = append(iters, o.lsm.NewIterator(opt)...)

	res := &DBIterator{
		vlog: o.vlog,
		iitr: lsm.NewMergeIterator(iters, opt.IsAsc),
	}
	return res
}

func (iter *DBIterator) Next() {
	iter.iitr.Next()
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	}
}
func (iter *DBIterator) Valid() bool {
	return iter.iitr.Valid()
}
func (iter *DBIterator) Rewind() {
	iter.iitr.Rewind()
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	}
}
func (iter *DBIterator) Item() utils2.Item {
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	e := iter.iitr.Item().Entry()
	var value []byte

	if e != nil && utils2.IsValuePtr(e) {
		var vp utils2.ValuePtr
		vp.Decode(e.Value)
		result, cb, err := iter.vlog.read(&vp)
		defer utils2.RunCallBack(cb)
		if err != nil {
			return nil
		}
		value = utils2.SafeCopy(nil, result)
	}

	if e.IsDeletedOrExpired() || value == nil {
		return nil
	}

	res := &utils2.Entry{
		Key:          e.Key,
		Value:        value,
		ExpiresAt:    e.ExpiresAt,
		Meta:         e.Meta,
		Version:      e.Version,
		Offset:       e.Offset,
		Hlen:         e.Hlen,
		ValThreshold: e.ValThreshold,
	}
	return res
}
func (iter *DBIterator) Close() error {
	return iter.iitr.Close()
}
func (iter *DBIterator) Seek(key []byte) {
}
