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
		// 如果第一次调用，需要默认置到block0
		b, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.initWithBlock(it, b)
		it.bi.seekFirst()
		it.err = it.bi.err
		return
	}
	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.item = it.bi.Item()
}

func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekFirst()
	} else {
		it.seekLast()
	}
}

func (b *blockIterator) initWithBlock(it *tableIterator, bl *block) {
	b.tableID = it.t.fid
	b.blockID = it.blockPos
	b.setBlock(bl)
}

func (it *tableIterator) seekFirst() {
	nums := len(it.t.sst.Index().GetOffsets())
	if nums == 0 {
		// 如果当前sst文件内block数目为0
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	b, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.initWithBlock(it, b)
	it.bi.seekFirst()
	it.item = it.bi.Item()
	it.err = it.bi.err
}

func (it *tableIterator) seekLast() {
	nums := len(it.t.sst.Index().GetOffsets())
	if nums == 0 {
		// 如果当前sst文件内block数目为0
		it.err = io.EOF
		return
	}
	it.blockPos = nums - 1
	b, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.initWithBlock(it, b)
	it.bi.seekLast()
	it.item = it.bi.Item()
	it.err = it.bi.err
}

func (it *tableIterator) Valid() bool {
	return it.err != io.EOF
}

func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
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
	if it.err == io.EOF {
		if idx == len(it.t.sst.Index().Offsets) {
			return
		}
		// 向后一个查
		it.seek(idx, key)
	}
}

func (it *tableIterator) seek(blockIndex int, key []byte) {
	it.blockPos = blockIndex
	block, err := it.t.block(blockIndex)
	if err != nil {
		it.err = err
		return
	}
	it.bi.initWithBlock(it, block)
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
	b.setIdx(b.idx + 1)
}

func (b *blockIterator) setBlock(block *block) {
	b.err = nil
	b.data = block.data[:block.entriesIndexStart]
	b.idx = 0
}

func (b *blockIterator) seekFirst() {
	b.setIdx(0)
}

func (b *blockIterator) seekLast() {
	b.setIdx(len(b.entryOffsets) - 1)
}

func (b *blockIterator) Rewind() {
	b.setIdx(0)
}

func (b *blockIterator) Valid() bool {
	return b.err != io.EOF
}

func (b *blockIterator) Close() error {
	return nil
}

func (b *blockIterator) Seek(key []byte) {
	entryNum := len(b.entryOffsets)
	start := 0
	entryIdx := sort.Search(entryNum, func(i int) bool {
		if i < start {
			return false
		}
		b.setIdx(i)
		return utils.CompareKeys(b.key, key) >= 0
	})
	b.setIdx(entryIdx)
}

func (b *blockIterator) Item() utils.Item {
	return b.it
}

// setIdx 设置当前索引下标为idx:第多少个entry
func (b *blockIterator) setIdx(idx int) {
	b.idx = idx
	// 下标越界
	if idx < 0 || idx >= len(b.entryOffsets) {
		b.err = io.EOF
		return
	}
	// Set base key.
	if len(b.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(b.data)
		b.baseKey = b.data[headerSize : headerSize+baseHeader.diff]
	}

	b.err = nil
	var (
		entryStartOffset int
		entryEndOffset   int
	)
	entryStartOffset = int(b.entryOffsets[idx])
	// set entry end offset
	if idx+1 == len(b.entryOffsets) {
		entryEndOffset = len(b.data)
	} else {
		entryEndOffset = int(b.entryOffsets[idx+1])
	}
	// | header(4 byte) | diff key | value-struct |
	entryData := b.data[entryStartOffset:entryEndOffset]
	var h header
	h.decode(entryData)
	// example: baseKey:"key"
	//          prevKey:"k1",prevOverlap:1
	//          currKey:"key1"
	// 			header:  overlap:3, diff:1
	// key = "k" + "ey" + "1"
	if h.overlap > b.prevOverlap {
		// todo figure out the logic of prev overlap key
		b.key = append(b.key[:b.prevOverlap], b.baseKey[b.prevOverlap:h.overlap]...)
	}
	b.prevOverlap = h.overlap
	valueOffset := headerSize + h.diff
	diffKey := b.data[headerSize:valueOffset]
	b.key = append(b.key[:h.overlap], diffKey...)

	entry := utils.NewEntry(b.key, nil)
	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOffset:])
	b.val = val.Value
	entry.Value = val.Value
	entry.ExpiresAt = val.ExpiresAt
	entry.Meta = val.Meta
	b.it = &Item{e: entry}
}
