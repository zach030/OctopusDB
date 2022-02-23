package lsm

import (
	"unsafe"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block // 当前的block
	cfg           *Config
	blockList     []*block // 本sst文件内的block列表
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64 // 当前最大版本号
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	offset            int    //当前block 在sst的offset 首地址
	checksum          []byte // block data md5
	entriesIndexStart int
	chkLen            int    // length of checksum
	data              []byte // all data in curr block
	baseKey           []byte // 此block的第一个key
	entryOffsets      []uint32
	end               int // current offset
	estimateSz        int64
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

func (h *header) decode(buf []byte) {
	copy((*[headerSize]byte)(unsafe.Pointer(h))[:], buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func newTableBuilder(cfg *Config) *tableBuilder {
	return &tableBuilder{
		cfg:     cfg,
		sstSize: cfg.SSTableMaxSz, // sst max size
	}
}

func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := utils.ValueStruct{Value: e.Value}
	// todo 判断此block是否已写完
	// try to allocate a block: modify curr block
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	if version := utils.ParseTimeStamp(key); version > tb.maxVersion {
		tb.maxVersion = version
	}
	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	h := header{
		overlap: uint16(len(key) - len(diffKey)), // overlap with common prefix
		diff:    uint16(len(diffKey)),            // different suffix length
	}

	// every block entry offset
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}

func (tb *tableBuilder) flush(lm *LevelManager, tableName string) (tbl *table, err error) {
	return
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	// basekey: "test"
	// newKey:  "test100"
	// diff key is : "100"
	return newKey[i:]
}

func (tb *tableBuilder) allocate(need int) []byte {
	// append data to curr block and modify offset
	// check data slice length
	b := tb.curBlock
	if len(b.data[b.end:]) < need {
		sz := 2 * len(b.data)
		if b.end+need > sz {
			sz = b.end + need
		}
		tmp := make([]byte, sz)
		copy(tmp, b.data)
		b.data = tmp
	}
	b.end += need
	return b.data[b.end-need : b.end]
}

func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	copy(dst, data)
}

func (tb *tableBuilder) done() buildData {
	bd := buildData{
		blockList: tb.blockList,
	}
	return bd
}
