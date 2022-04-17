package kv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	file2 "github.com/zach030/OctopusDB/kv/file"
	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/prometheus/common/log"

	"github.com/pkg/errors"
)

const discardStatsFlushThreshold = 100

var (
	lfDiscardStatsKey = []byte("!octopus!discard") // For storing lfDiscardStats

	head = []byte("!octopus!head") // For storing value offset for replay.
)

type valueLog struct {
	dirPath string
	// guards our view of which files exist, which to be deleted, how many active iterators
	filesLock        sync.RWMutex
	filesMap         map[uint32]*file2.LogFile
	maxFid           uint32
	filesToBeDeleted []uint32
	// A refcount of iterators -- when this hits zero, we can delete the filesToBeDeleted.
	numActiveIterators int32
	db                 *OctopusDB
	writableLogOffset  uint32 // read by read, written by write. Must access via atomics.
	numEntriesWritten  uint32
	opt                Options
	garbageCh          chan struct{}
	lfDiscardStats     *lfDiscardStats
}

// lfDiscardStats 记录丢弃key的数据
// lfDiscardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type lfDiscardStats struct {
	sync.RWMutex
	m                 map[uint32]int64
	flushChan         chan map[uint32]int64
	closer            *utils2.Closer
	updatesSinceFlush int
}

func (vlog *valueLog) newValuePtr(e *utils2.Entry) (*utils2.ValuePtr, error) {
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = []*utils2.Entry{e}
	req.Wg.Add(1)
	req.IncrRef() // for db write
	defer req.DecrRef()
	err := vlog.write([]*request{req})
	return req.Ptrs[0], err
}

func (vlog *valueLog) open(ptr *utils2.ValuePtr, replayFn utils2.LogEntry) error {
	vlog.lfDiscardStats.closer.Add(1)
	go vlog.flushDiscardStats()
	// 统计所有vlog文件
	if err := vlog.populateFilesMap(); err != nil {
		return err
	}
	// If no files are found, then create a new file.
	if len(vlog.filesMap) == 0 {
		// 构造000.vlog文件
		_, err := vlog.createVlogFile(0)
		return errors.Wrapf(err, "Error while creating log file in valueLog.open")
	}
	// 对文件进行排序
	fids := vlog.sortedFids()
	for _, fid := range fids {
		lf, ok := vlog.filesMap[fid]
		if !ok {
			panic(fmt.Errorf("vlog.filesMap[fid] fid not found"))
		}
		// 打开mmap映射的vlog文件
		var err error
		if err = lf.Open(
			&file2.Option{
				FID:      uint64(fid),
				FileName: vlog.fpath(fid),
				Dir:      vlog.dirPath,
				Path:     vlog.dirPath,
				MaxSz:    2 * vlog.db.opt.ValueLogFileSize,
			}); err != nil {
			return errors.Wrapf(err, "Open existing file: %q", lf.FileName())
		}
		var offset uint32
		// 从head处开始重放vlog日志，而不是从第一条日志
		// head 相当于一个快照
		if fid == ptr.Fid {
			offset = ptr.Offset + ptr.Len
		}
		log.Infof("Replaying file id: %d at offset: %d\n", fid, offset)
		now := time.Now()
		// 对此vlog文件进行重放日志
		if err := vlog.replayLog(lf, offset, replayFn); err != nil {
			// Log file is corrupted. Delete it.
			if err == utils2.ErrDeleteVlogFile {
				delete(vlog.filesMap, fid)
				// Close the fd of the file before deleting the file otherwise windows complaints.
				if err := lf.Close(); err != nil {
					return errors.Wrapf(err, "failed to close vlog file %s", lf.FileName())
				}
				path := vlog.fpath(lf.FID)
				if err := os.Remove(path); err != nil {
					return errors.Wrapf(err, "failed to delete empty value log file: %q", path)
				}
				continue
			}
			return err
		}
		log.Infof("Replay took: %s\n", time.Since(now))

		if fid < vlog.maxFid {
			// This file has been replayed. It can now be mmapped.
			// For maxFid, the mmap would be done by the specially written code below.
			if err := lf.Init(); err != nil {
				return err
			}
		}
	}
	// 设置当前vlog的写指针
	// Seek to the end to start writing.
	last, ok := vlog.filesMap[vlog.maxFid]
	if !ok {
		panic(errors.New("vlog.filesMap[vlog.maxFid] not found"))
	}
	lastOffset, err := last.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("file.Seek to end path:[%s]", last.FileName()))
	}
	vlog.writableLogOffset = uint32(lastOffset)

	// head的设计起到check point的作用
	vlog.db.vhead = &utils2.ValuePtr{Fid: vlog.maxFid, Offset: uint32(lastOffset)}
	if err := vlog.populateDiscardStats(); err != nil {
		log.Error(fmt.Errorf("Failed to populate discard stats: %s\n", err))
	}
	return nil
}

// populateFilesMap 填充文件目录下的所有vlog文件
func (vlog *valueLog) populateFilesMap() error {
	vlog.filesMap = make(map[uint32]*file2.LogFile)
	files, err := ioutil.ReadDir(vlog.dirPath)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Unable to open log dir. path[%s]", vlog.dirPath))
	}
	found := make(map[uint64]struct{})
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".vlog") {
			continue
		}
		fsz := len(f.Name())
		fid, err := strconv.ParseUint(f.Name()[:fsz-5], 10, 32)
		if err != nil {
			return errors.Wrapf(err, fmt.Sprintf("Unable to parse log id. name:[%s]", f.Name()))
		}
		if _, ok := found[fid]; ok {
			return errors.Wrapf(err, fmt.Sprintf("Duplicate file found. Please delete one. name:[%s]", f.Name()))
		}
		found[fid] = struct{}{}

		lf := &file2.LogFile{
			FID:  uint32(fid),
			Lock: sync.RWMutex{},
		}
		vlog.filesMap[uint32(fid)] = lf
		if vlog.maxFid < uint32(fid) {
			vlog.maxFid = uint32(fid)
		}
	}
	return nil
}

// createVlogFile 创建指定fid的vlog文件
func (vlog *valueLog) createVlogFile(fid uint32) (*file2.LogFile, error) {
	path := vlog.fpath(fid)

	lf := &file2.LogFile{
		FID:  fid,
		Lock: sync.RWMutex{},
	}

	var err error
	if err = lf.Open(&file2.Option{
		FID:      uint64(fid),
		FileName: path,
		Dir:      vlog.dirPath,
		Path:     vlog.dirPath,
		MaxSz:    2 * vlog.db.opt.ValueLogFileSize,
	}); err != nil {
		panic(err)
	}
	removeFile := func() {
		// 如果处理出错 则直接删除文件
		err = os.Remove(lf.FileName())
		return
	}

	if err = lf.Bootstrap(); err != nil {
		removeFile()
		return nil, err
	}

	if err = utils2.SyncDir(vlog.dirPath); err != nil {
		removeFile()
		return nil, errors.Wrapf(err, fmt.Sprintf("Sync value log dir[%s]", vlog.dirPath))
	}
	vlog.filesLock.Lock()
	vlog.filesMap[fid] = lf
	vlog.maxFid = fid
	// 现在header才是0
	atomic.StoreUint32(&vlog.writableLogOffset, utils2.VlogHeaderSize)
	vlog.numEntriesWritten = 0
	vlog.filesLock.Unlock()
	return lf, nil
}

// sortedFids 对vlog文件按照文件名大小进行排序，排除正在处于gc状态的文件
func (vlog *valueLog) sortedFids() []uint32 {
	toDel := make(map[uint32]struct{})
	// 排除处于gc状态的vlog文件
	for _, u := range vlog.filesToBeDeleted {
		toDel[u] = struct{}{}
	}
	toSort := make([]uint32, 0, len(vlog.filesMap))
	for id := range vlog.filesMap {
		if _, ok := toDel[id]; !ok {
			toSort = append(toSort, id)
		}
	}
	sort.Slice(toSort, func(i, j int) bool {
		return toSort[i] < toSort[j]
	})
	return toSort
}

// replayLog 重放vlog
func (vlog *valueLog) replayLog(lf *file2.LogFile, offset uint32, replayFn utils2.LogEntry) error {
	// Alright, let's iterate now.
	endOffset, err := lf.Iterate(offset, replayFn)
	if err != nil {
		return errors.Wrapf(err, "Unable to replay logfile:[%s]", lf.FileName())
	}
	if int64(endOffset) == lf.Size() {
		return nil
	}

	// TODO: 如果vlog日志损坏怎么办? 当前默认是截断损坏的数据

	// The entire file should be truncated (i.e. it should be deleted).
	// If fid == maxFid then it's okay to truncate the entire file since it will be
	// used for future additions. Also, it's okay if the last file has size zero.
	// We mmap 2*opt.ValueLogSize for the last file. See vlog.Open() function
	// if endOffset <= vlogHeaderSize && lf.fid != vlog.maxFid {

	if endOffset <= utils2.VlogHeaderSize {
		if lf.FID != vlog.maxFid {
			return utils2.ErrDeleteVlogFile
		}
		return lf.Bootstrap()
	}

	log.Infof("Truncating vlog file %s to offset: %d\n", lf.FileName(), endOffset)
	if err := lf.Truncate(int64(endOffset)); err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset))
	}
	return nil
}

func (vlog *valueLog) read(vp *utils2.ValuePtr) ([]byte, func(), error) {
	buf, lf, err := vlog.readValueBytes(vp)
	cb := vlog.getUnlockCallback(lf)
	if err != nil {
		return nil, cb, err
	}
	if vlog.opt.VerifyValueChecksum {
		hash := crc32.New(utils2.CastagnoliCrcTable)
		if _, err := hash.Write(buf[:len(buf)-crc32.Size]); err != nil {
			utils2.RunCallBack(cb)
			return nil, nil, errors.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		// Fetch checksum from the end of the buffer.
		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != utils2.BytesToU32(checksum) {
			utils2.RunCallBack(cb)
			return nil, nil, errors.Wrapf(utils2.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}
	var h utils2.Header
	headerLen := h.Decode(buf)
	kv := buf[headerLen:]
	if uint32(len(kv)) < h.KLen+h.VLen {
		log.Errorf("Invalid read: vp: %+v\n", vp)
		return nil, nil, errors.Errorf("Invalid read: Len: %d read at:[%d:%d]", len(kv), h.KLen, h.KLen+h.VLen)
	}
	return kv[h.KLen : h.KLen+h.VLen], cb, nil
}

// readValueBytes return vlog entry slice and read locked log file. Caller should take care of
// logFile unlocking.
func (vlog *valueLog) readValueBytes(vp *utils2.ValuePtr) ([]byte, *file2.LogFile, error) {
	// 根据值指针读vlog，首先根据id选出所在的vlog
	lf, err := vlog.getVLogFile(vp)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	// 使用vlog底层根据offset，len读取data
	buf, err := lf.Read(vp)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	return buf, lf, nil
}

func (vlog *valueLog) getVLogFile(vp *utils2.ValuePtr) (*file2.LogFile, error) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	lf, ok := vlog.filesMap[vp.Fid]
	if !ok {
		return nil, fmt.Errorf("value-log with id:%v is not found", vp.Fid)
	}
	// 校验是否超出vlog可读范围
	maxLf := vlog.maxFid
	if vp.Fid == maxLf {
		if vp.Offset >= vlog.woffset() {
			return nil, errors.Errorf(
				"Invalid value pointer offset: %d greater than current offset: %d",
				vp.Offset, vlog.woffset())
		}
	}
	lf.Lock.RLock()
	return lf, nil
}

// getUnlockCallback will returns a function which unlock the logfile if the logfile is mmaped.
// otherwise, it unlock the logfile and return nil.
func (vlog *valueLog) getUnlockCallback(lf *file2.LogFile) func() {
	if lf == nil {
		return nil
	}
	return lf.Lock.RUnlock
}

func (vlog *valueLog) write(reqs []*request) error {
	if err := vlog.validateWrites(reqs); err != nil {
		return err
	}
	vlog.filesLock.RLock()
	currLogFile := vlog.filesMap[vlog.maxFid]
	vlog.filesLock.RUnlock()
	var buf bytes.Buffer

	flushWrites := func() error {
		if buf.Len() == 0 {
			return nil
		}
		data := buf.Bytes()
		offset := vlog.woffset()
		if err := currLogFile.Write(offset, data); err != nil {
			return errors.Wrapf(err, "Unable to write to value log file: %q", currLogFile.FileName())
		}
		buf.Reset()
		atomic.AddUint32(&vlog.writableLogOffset, uint32(len(data)))
		currLogFile.AddSize(vlog.writableLogOffset)
		return nil
	}
	toDisk := func() error {
		if err := flushWrites(); err != nil {
			return err
		}
		// 切分vlog文件
		if vlog.woffset() > uint32(vlog.opt.ValueLogFileSize) || vlog.numEntriesWritten > vlog.opt.ValueLogMaxEntries {
			if err := currLogFile.DoneWriting(vlog.woffset()); err != nil {
				return err
			}

			newid := atomic.AddUint32(&vlog.maxFid, 1)
			if newid <= 0 {
				panic(fmt.Errorf("newid has overflown uint32: %v", newid))
			}
			// 创建新的vlog文件
			newlf, err := vlog.createVlogFile(newid)
			if err != nil {
				return err
			}
			currLogFile = newlf
			atomic.AddInt32(&vlog.db.logRotates, 1)
		}
		return nil
	}

	for i := range reqs {
		b := reqs[i]
		b.Ptrs = b.Ptrs[0:]
		nums := 0
		for j := range b.Entries {
			en := b.Entries[j]
			if vlog.db.shouldWriteValueToLSM(en) {
				b.Ptrs = append(b.Ptrs, &utils2.ValuePtr{})
				continue
			}
			var p utils2.ValuePtr
			p.Fid = currLogFile.FID
			p.Offset = vlog.woffset() + uint32(buf.Len())
			plen, err := currLogFile.EncodeEntry(en, &buf, p.Offset)
			if err != nil {
				log.Error(err)
				return err
			}
			p.Len = uint32(plen)
			b.Ptrs = append(b.Ptrs, &p)
			nums++
			if buf.Len() > vlog.db.opt.ValueLogFileSize {
				if err := flushWrites(); err != nil {
					return err
				}
			}
		}
		vlog.numEntriesWritten = uint32(nums)
		// We write to disk here so that all entries that are part of the same transaction are
		// written to the same vlog file.
		writeNow := vlog.woffset()+uint32(buf.Len()) > uint32(vlog.opt.ValueLogFileSize) ||
			vlog.numEntriesWritten > vlog.opt.ValueLogMaxEntries
		if writeNow {
			if err := toDisk(); err != nil {
				return err
			}
		}
	}
	return toDisk()
}

// validateWrites 检查当前request内的entry能否写入vlog文件
func (vlog *valueLog) validateWrites(reqs []*request) error {
	currOffset := uint64(vlog.woffset())
	for _, req := range reqs {
		size := estimateRequestSize(req)
		aboutSize := currOffset + size
		if aboutSize > uint64(utils2.MaxVlogFileSize) {
			return errors.Errorf("Request size offset %d is bigger than maximum offset %d",
				aboutSize, utils2.MaxVlogFileSize)
		}
		if aboutSize >= uint64(vlog.opt.ValueLogFileSize) {
			// We'll create a new vlog file if the estimated offset is greater or equal to
			// max vlog size. So, resetting the vlogOffset.
			currOffset = 0
			continue
		}
		// Estimated vlog offset will become current vlog offset if the vlog is not rotated.
		currOffset = aboutSize
	}
	return nil
}

// estimateRequestSize returns the size that needed to be written for the given request.
func estimateRequestSize(req *request) uint64 {
	size := uint64(0)
	for _, e := range req.Entries {
		size += uint64(utils2.MaxHeaderSize + len(e.Key) + len(e.Value) + crc32.Size)
	}
	return size
}

func (vlog *valueLog) woffset() uint32 {
	return atomic.LoadUint32(&vlog.writableLogOffset)
}

// 请求池
var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

// request
type request struct {
	// Input values
	Entries []*utils2.Entry
	// Output values and wait group stuff below
	Ptrs []*utils2.ValuePtr
	Wg   sync.WaitGroup
	Err  error
	ref  int32
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
	req.ref = 0
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.Entries = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}

func (o *OctopusDB) InitVlog() {
	h, _ := o.getHead()
	vlog := &valueLog{
		dirPath:          o.opt.WorkDir,
		filesToBeDeleted: make([]uint32, 0),
		lfDiscardStats: &lfDiscardStats{
			m:         make(map[uint32]int64),
			closer:    utils2.NewCloser(),
			flushChan: make(chan map[uint32]int64, 16),
		},
	}
	vlog.db = o
	vlog.opt = *o.opt
	vlog.garbageCh = make(chan struct{}, 1)
	if err := vlog.open(h, o.replayFunction()); err != nil {
		panic(err)
	}
	o.vlog = vlog
}

// getHead checkpoint of all value-log
func (o *OctopusDB) getHead() (*utils2.ValuePtr, uint64) {
	var vptr utils2.ValuePtr
	return &vptr, 0
}

// replayFunction 重放vlog中的entry 保证lsm与vlog数据的一致性
func (o *OctopusDB) replayFunction() func(*utils2.Entry, *utils2.ValuePtr) error {
	// 将entry写入lsm
	toLSM := func(k []byte, vs utils2.ValueStruct) {
		o.lsm.Set(&utils2.Entry{
			Key:       k,
			Value:     vs.Value,
			ExpiresAt: vs.ExpiresAt,
			Meta:      vs.Meta,
		})
	}
	return func(e *utils2.Entry, vp *utils2.ValuePtr) error { // Function for replaying.
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.Meta
		if o.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = vp.Encode()
			meta = meta | utils2.BitValuePointer
		}
		// Update vhead. If the crash happens while replay was in progess
		// and the head is not updated, we will end up replaying all the
		// files starting from file zero, again.
		o.updateHead([]*utils2.ValuePtr{vp})

		v := utils2.ValueStruct{
			Value:     nv,
			Meta:      meta,
			ExpiresAt: e.ExpiresAt,
		}
		// This entry is from a rewrite or via SetEntryAt(..).
		toLSM(nk, v)
		return nil
	}
}

// updateHead should not be called without the db.Lock() since db.vhead is used
// by the writer go routines and memtable flushing goroutine.
func (o *OctopusDB) updateHead(ptrs []*utils2.ValuePtr) {
	var ptr *utils2.ValuePtr
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}
	if ptr.Less(o.vhead) {
		panic(fmt.Errorf("ptr.Less(db.vhead) is true"))
	}
	o.vhead = ptr
}

func (vlog *valueLog) fpath(fid uint32) string {
	return utils2.VlogFilePath(vlog.dirPath, fid)
}

func (vlog *valueLog) flushDiscardStats() {
	defer vlog.lfDiscardStats.closer.Done()

	mergeStats := func(stats map[uint32]int64) ([]byte, error) {
		vlog.lfDiscardStats.Lock()
		defer vlog.lfDiscardStats.Unlock()
		for fid, count := range stats {
			vlog.lfDiscardStats.m[fid] += count
			vlog.lfDiscardStats.updatesSinceFlush++
		}

		if vlog.lfDiscardStats.updatesSinceFlush > discardStatsFlushThreshold {
			encodedDS, err := json.Marshal(vlog.lfDiscardStats.m)
			if err != nil {
				return nil, err
			}
			vlog.lfDiscardStats.updatesSinceFlush = 0
			return encodedDS, nil
		}
		return nil, nil
	}

	process := func(stats map[uint32]int64) error {
		encodedDS, err := mergeStats(stats)
		if err != nil || encodedDS == nil {
			return err
		}

		entries := []*utils2.Entry{{
			Key:   utils2.KeyWithTs(lfDiscardStatsKey, 1),
			Value: encodedDS,
		}}
		req, err := vlog.db.sendToWriteCh(entries)
		// No special handling of ErrBlockedWrites is required as err is just logged in
		// for loop below.
		if err != nil {
			return errors.Wrapf(err, "failed to push discard stats to write channel")
		}
		return req.Wait()
	}

	closer := vlog.lfDiscardStats.closer
	for {
		select {
		case <-closer.CloseSignal:
			// For simplicity just return without processing already present in stats in flushChan.
			return
		case stats := <-vlog.lfDiscardStats.flushChan:
			if err := process(stats); err != nil {
				log.Error(fmt.Errorf("unable to process discardstats with error: %s", err))
				return
			}
		}
	}
}

// 统计脏数据
func (vlog *valueLog) populateDiscardStats() error {
	//key := utils.KeyWithTs(lfDiscardStatsKey, math.MaxUint64)
	//var statsMap map[uint32]int64
	//vs, err := vlog.db.Get(key)
	//if err != nil {
	//	return err
	//}
	//// Value doesn't exist.
	//if vs.Meta == 0 && len(vs.Value) == 0 {
	//	return nil
	//}
	//val := vs.Value
	//// Entry is not stored in the LSM tree.
	//if utils.IsValuePtr(vs) {
	//	var vp utils.ValuePtr
	//	vp.Decode(val)
	//	// Read entry from the value log.
	//	result, cb, err := vlog.read(&vp)
	//	// Copy it before we release the read lock.
	//	val = utils.SafeCopy(nil, result)
	//	utils.RunCallback(cb)
	//	if err != nil {
	//		return err
	//	}
	//}
	//if len(val) == 0 {
	//	return nil
	//}
	//if err := json.Unmarshal(val, &statsMap); err != nil {
	//	return errors.Wrapf(err, "failed to unmarshal discard stats")
	//}
	//fmt.Printf("Value Log Discard stats: %v\n", statsMap)
	//vlog.lfDiscardStats.flushChan <- statsMap
	return nil
}

func (vlog *valueLog) close() error {
	if vlog == nil || vlog.db == nil {
		return nil
	}
	// close flushDiscardStats.
	<-vlog.lfDiscardStats.closer.CloseSignal
	var err error
	for id, f := range vlog.filesMap {
		f.Lock.Lock() // We won’t release the lock.
		maxFid := vlog.maxFid
		// TODO(ibrahim) - Do we need the following truncations on non-windows
		// platforms? We expand the file only on windows and the vlog.woffset()
		// should point to end of file on all other platforms.
		if id == maxFid {
			// truncate writable log file to correct offset.
			if truncErr := f.Truncate(int64(vlog.woffset())); truncErr != nil && err == nil {
				err = truncErr
			}
		}
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		f.Lock.Unlock()
	}
	return err
}
