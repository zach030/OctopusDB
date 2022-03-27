package lsm

import (
	"math"
	"os"
	"unsafe"

	"github.com/zach030/OctopusDB/internal/kv/file"

	"github.com/zach030/OctopusDB/internal/kv/pb"

	"github.com/pkg/errors"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block // 当前的block
	cfg           *Config
	blockList     []*block // 本sst文件内的block列表
	keyCount      uint32   // key数量
	keyHashes     []uint32 // keyHash的数组
	maxVersion    uint64   // 当前最大版本号
	baseKey       []byte   // 初始key
	staleDataSize int
	estimateSz    int64
}

// buildData short info about one sst
type buildData struct {
	blockList []*block // block list
	index     []byte   // block index slice
	checksum  []byte   // checksum of content
	size      int      // size of checksum
}

type block struct {
	offset            int    //当前block 在sst的offset 首地址
	checksum          []byte // block data md5
	entriesIndexStart int
	chkLen            int      // length of checksum
	data              []byte   // all data in curr block
	baseKey           []byte   // first key in curr block
	entryOffsets      []uint32 // record each entry offset
	end               int      // current offset
	estimateSz        int64    // estimate current entry size to try to allocate new block
}

// Decode
// block structure:
// | entry1-offset | entry2-offset | ...... | length of entryOffsets | checksum | length of checksum |
func (b *block) Decode() error {
	pos := len(b.data) - 4
	b.chkLen = int(utils.BytesToU32(b.data[pos : pos+4]))
	pos -= b.chkLen
	b.checksum = b.data[pos : pos+b.chkLen]
	pos -= 4
	NumOfEntries := int(utils.BytesToU32(b.data[pos : pos+4]))
	b.entriesIndexStart = pos - (NumOfEntries * 4)
	entriesEndOffset := b.entriesIndexStart + NumOfEntries*4
	b.entryOffsets = utils.BytesToU32Slice(b.data[b.entriesIndexStart:entriesEndOffset])
	// todo 为什么pos+4，numsOfEntries也算data吗
	b.data = b.data[:pos+4]
	if err := utils.VerifyChecksum(b.data, b.checksum); err != nil {
		return err
	}
	return nil
}

type header struct {
	overlap uint16 // Overlap with base key. 2 byte
	diff    uint16 // Length of the diff. 2 byte
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

func newTableBuilderWithSSTSize(opt *Config, size int64) *tableBuilder {
	return &tableBuilder{
		cfg:     opt,
		sstSize: size,
	}
}

func newTableBuilder(cfg *Config) *tableBuilder {
	return &tableBuilder{
		cfg:     cfg,
		sstSize: cfg.SSTableMaxSz, // sst max size
	}
}

func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }

func (tb *tableBuilder) finish() []byte {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	if written == len(buf) {
		panic("written == len(buf)")
	}
	return buf
}

// Close closes the TableBuilder.
func (tb *tableBuilder) Close() {
	// 结合内存分配器
}

// AddStaleKey 记录陈旧key所占用的空间大小，用于日志压缩时的决策
func (tb *tableBuilder) AddStaleKey(e *utils.Entry) {
	// Rough estimate based on how much space it will occupy in the SST.
	tb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, true)
}

// AddKey _
func (tb *tableBuilder) AddKey(e *utils.Entry) {
	tb.add(e, false)
}

func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := utils.ValueStruct{Value: e.Value, ExpiresAt: e.ExpiresAt, Meta: e.Meta}
	// add current entry, try to allocate new block
	if tb.tryFinishBlock(e) {
		if isStale {
			// This key will be added to tableIndex and it is stale.
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		// Create a new block and start writing.
		tb.curBlock = &block{
			data: make([]byte, tb.cfg.BlockSize),
		}
	}
	// try to allocate a block: modify curr block
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	if version := utils.ParseTimeStamp(key); version > tb.maxVersion {
		tb.maxVersion = version
	}
	var diffKey []byte
	// 如果没有设置basekey的话，将当前的key设置为baseKey
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	h := header{
		// 与baseKey重叠的长度 + 不同的key长度
		overlap: uint16(len(key) - len(diffKey)), // overlap with common prefix
		diff:    uint16(len(diffKey)),            // different suffix length
	}

	// every block entry offset
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	// | header(4 byte) | diff key | value-struct |
	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}

func (tb *tableBuilder) flush(lm *LevelManager, tableName string) (tbl *table, err error) {
	bd := tb.done()
	tbl = &table{manager: lm, fid: utils.FID(tableName)}
	tbl.sst = file.OpenSSTable(&file.Option{
		FileName: tableName,
		Dir:      lm.cfg.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    bd.size,
	})
	// store full block byte data to buf
	buf := make([]byte, bd.size)
	if written := bd.Copy(buf); written != len(buf) {
		panic("tableBuilder.flush written != len(buf)")
	}
	// 从sst文件映射的mmap中分配一块db.size大小的空间，用于写入sst文件
	sstDst, err := tbl.sst.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(sstDst, buf)
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

// allocate try to allocate need space for entry
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

// append serialized data to current block
func (tb *tableBuilder) append(data []byte) {
	// allocate length of data in curr block
	dst := tb.allocate(len(data))
	// copy data to dst space
	copy(dst, data)
}

// tryFinishBlock if current block is full
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		// not any entry in current block
		return false
	}
	if !((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32) {
		panic(errors.New("Integer overflow"))
	}
	// 当前block的指针为end，这里预估加上当前entry后会不会超出BlockSize
	// 当前entry的size包括：offset（索引信息+header）+ keySize + valueSize
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	if !(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32) {
		panic(errors.New("Integer overflow"))
	}

	return tb.curBlock.estimateSz > int64(tb.cfg.BlockSize)
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

// finishBlock finish a block to sst
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Already write all entry kv data, write offsets and checksum now
	// Append the entryOffsets and its length.
	// | entry1-offset | entry2-offset | ...... | length of entryOffsets | checksum | length of checksum |
	// block encode
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])
	// Append the block checksum and its length.
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))

	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil
	return
}

// Copy 将bd中所有数据序列化到dst中
func (bd *buildData) Copy(dst []byte) int {
	var written int
	// 1. copy block data
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	// 2. copy blocks index
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))
	// 2. copy checksum
	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// done 当一个sst文件写完毕时，调用done，返回buildData：对sst的缩略信息，记录了block列表，block索引，checksum以及文件size
func (tb *tableBuilder) done() buildData {
	// try to finish current block
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}
	var f utils.BloomFilter
	// has bloom filter
	if tb.cfg.BloomFalsePositive > 0 {
		bits := utils.BitsPerKey(len(tb.keyHashes), tb.cfg.BloomFalsePositive)
		f = utils.NewBloomFilter(tb.keyHashes, bits)
	}
	// get blocks index and data size by filter
	// table offset: | base-key , start-offset, len |
	index, dataSize := tb.buildIndex(f.Filter)
	// 记录checksum
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

// buildIndex 构建此sst文件内所有block的索引
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets()
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	if err != nil {
		panic(err)
	}
	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets() []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (tb *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

func (tb *tableBuilder) ReachedCapacity() bool {
	return tb.estimateSz > tb.sstSize
}
