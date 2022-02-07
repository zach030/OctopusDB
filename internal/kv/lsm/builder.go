package lsm

import "github.com/zach030/OctopusDB/internal/kv/utils"

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	cfg           *Config
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
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
	offset            int //当前block的offset 首地址
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	estimateSz        int64
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

func newTableBuilder(cfg *Config) *tableBuilder {
	return &tableBuilder{
		cfg:     cfg,
		sstSize: cfg.SSTableMaxSz,
	}
}

func (t *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := utils.ValueStruct{Value: e.Value}
	// try to allocate a block: modify curr block
	t.keyHashes = append(t.keyHashes, utils.Hash(utils.ParseKey(key)))
	if version := utils.ParseTimeStamp(key); version > t.maxVersion {
		t.maxVersion = version
	}
	var diffKey []byte
	if len(t.curBlock.baseKey) == 0 {
		t.curBlock.baseKey = append(t.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = t.keyDiff(key)
	}
	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	t.curBlock.entryOffsets = append(t.curBlock.entryOffsets, uint32(t.curBlock.end))

	//t.append(h.encode())
	//t.append(diffKey)

	//dst := t.allocate(int(val.EncodedSize()))
	//val.EncodeValue(dst)
}

func (t *tableBuilder) flush(lm *LevelManager, tableName string) (tbl *table, err error) {
	return
}

func (t *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(t.curBlock.baseKey); i++ {
		if newKey[i] != t.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (t *tableBuilder) append(data []byte) {

}
