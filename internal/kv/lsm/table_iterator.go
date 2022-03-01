package lsm

import (
	"bytes"
	"io"
	"sort"

	"github.com/zach030/OctopusDB/internal/kv/pb"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

// tableIterator curr-table-->curr-block-->curr-entry
type tableIterator struct {
	item     utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

func (it *tableIterator) Next() {
	it.err = nil
	tblOffsets := it.t.sst.Index().GetOffsets()
	if it.blockPos >= len(tblOffsets) {
		it.err = io.EOF
		return
	}
	if len(it.bi.data) == 0 {

	}
}

func (it *tableIterator) Rewind() {
	panic("implement me")
}

func (it *tableIterator) Valid() bool {
	return it.err != io.EOF
}

func (it *tableIterator) Close() error {
	return nil
}

func (it *tableIterator) Seek(key []byte) {
	// 根据key与blockOffset比较，判断属于哪个block
	var (
		bo  *pb.BlockOffset
		ok  bool
		idx int
	)
	// 找到所在block的索引
	idx = sort.Search(len(it.t.sst.Index().GetOffsets()), func(i int) bool {
		bo, ok = it.t.getBlockOffset(i)
		if !ok {
			panic("block offset panic")
		}
		return bytes.Compare(bo.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seek(0, key)
	}
	it.seek(idx-1, key)
}

func (it *tableIterator) seek(blockIndex int, key []byte) {
	it.blockPos = blockIndex
	block, err := it.t.block(blockIndex)
	if err != nil {
		it.err = err
		return
	}
	it.bi.blockID = it.blockPos
	it.bi.tableID = it.t.fid
	it.bi.setBlock(block)
	it.bi.Seek(key)

	it.err = it.bi.err
	it.item = it.bi.Item()
}

func (it *tableIterator) Item() utils.Item {
	return it.item
}

type blockIterator struct {
	data         []byte // block data
	idx          int
	err          error
	baseKey      []byte // base key of block
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}

func (b *blockIterator) Next() {
	panic("implement me")
}

func (b *blockIterator) setBlock(block *block) {
	b.err = nil
	b.data = block.data[:block.entriesIndexStart]
	b.idx = 0
}

func (b *blockIterator) Rewind() {
	panic("implement me")
}

func (b *blockIterator) Valid() bool {
	return b.err != io.EOF
}

func (b *blockIterator) Close() error {
	return nil
}

func (b *blockIterator) Seek(bytes []byte) {

}

func (b *blockIterator) Item() utils.Item {
	return b.it
}
