package kv

import (
	"expvar"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/log"

	"github.com/pkg/errors"

	"github.com/zach030/OctopusDB/internal/kv/lsm"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type API interface {
	Set(data *utils.Entry) error
	Get(key []byte) (*utils.Entry, error)
	Del(key []byte) error
	NewIterator(opt *Options) utils.Iterator
	Info() *stat
	Close() error
}

type OctopusDB struct {
	sync.RWMutex
	lsm         *lsm.LSM
	vlog        *valueLog
	opt         *Options
	stat        *stat
	flushChan   chan flushTask // For flushing memtables.
	writeCh     chan *request
	blockWrites int32
	vhead       *utils.ValuePtr // vlog同步数据截断点
	logRotates  int32
}

func Open(opt *Options) *OctopusDB {
	c := utils.NewCloser()
	db := &OctopusDB{opt: opt}
	// todo 目录锁
	db.InitVlog()
	db.lsm = lsm.NewLSM(&lsm.Config{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0, //0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
		DiscardStatsCh:      &(db.vlog.lfDiscardStats.flushChan),
	})
	db.stat = newStat()
	//go db.lsm.StartCompaction()
	c.Add(1)
	db.writeCh = make(chan *request)
	db.flushChan = make(chan flushTask, 16)
	go db.doWrites(c)
	go db.stat.StartStat()
	return db
}

func (o *OctopusDB) Set(data *utils.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 1. 判断value大小
	var vp *utils.ValuePtr
	var err error
	data.Key = utils.KeyWithTs(data.Key, math.MaxUint32)
	if !o.shouldWriteValueToLSM(data) {
		if vp, err = o.vlog.newValuePtr(data); err != nil {
			return err
		}
		data.Meta |= utils.BitValuePointer
		data.Value = vp.Encode()
	}
	return o.lsm.Set(data)
}

func (o *OctopusDB) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	var (
		entry *utils.Entry
		err   error
	)
	// 1. 先从lsm拿key
	// 2. 如果存了vlog，再取value
	key = utils.KeyWithTs(key, math.MaxUint32)
	if entry, err = o.lsm.Get(key); err != nil {
		return entry, err
	}
	// 3. 判断是否存vlog
	if entry != nil && utils.IsValuePtr(entry) {
		var vp utils.ValuePtr
		vp.Decode(entry.Value)
		buf, cb, err := o.vlog.read(&vp)
		defer utils.RunCallBack(cb)
		if err != nil {
			return nil, err
		}
		entry.Value = utils.SafeCopy(nil, buf)
	}
	if isDeletedOrExpired(entry) {
		return nil, utils.ErrKeyNotFound
	}
	return entry, nil
}

// 判断是否过期 是可删除
func isDeletedOrExpired(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (o *OctopusDB) Del(key []byte) error {
	return o.Set(&utils.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (o *OctopusDB) Info() *stat {
	return o.stat
}

func (o *OctopusDB) Close() error {
	o.vlog.lfDiscardStats.closer.Close()
	if err := o.lsm.Close(); err != nil {
		return err
	}
	if err := o.vlog.close(); err != nil {
		return err
	}
	//if err := o.stats.close(); err != nil {
	//	return err
	//}
	return nil
}

// RunValueLogGC triggers a value log garbage collection.
func (o *OctopusDB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	// Find head on disk
	headKey := utils.KeyWithTs(head, math.MaxUint64)
	val, err := o.lsm.Get(headKey)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = &utils.Entry{
				Key:   headKey,
				Value: []byte{},
			}
		} else {
			return errors.Wrap(err, "Retrieving head from on-disk LSM")
		}
	}

	// 内部key head 一定是value ptr 不需要检查内容
	var head utils.ValuePtr
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	return o.vlog.runGC(discardRatio, &head)
}

func (o *OctopusDB) shouldWriteValueToLSM(e *utils.Entry) bool {
	return int64(len(e.Value)) < o.opt.ValueThreshold
}

func (o *OctopusDB) sendToWriteCh(entries []*utils.Entry) (*request, error) {
	if atomic.LoadInt32(&o.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}
	var count, size int64
	for _, e := range entries {
		size += int64(e.EstimateSize(int(o.opt.ValueThreshold)))
		count++
	}
	if count >= o.opt.MaxBatchCount || size >= o.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	// TODO 尝试使用对象复用，后面entry对象也应该使用
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()    // for db write
	o.writeCh <- req // Handled in doWrites.
	return req, nil
}

//   Check(kv.BatchSet(entries))
func (o *OctopusDB) batchSet(entries []*utils.Entry) error {
	req, err := o.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

func (o *OctopusDB) doWrites(lc *utils.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := o.writeRequests(reqs); err != nil {
			log.Errorf("writeRequests: %v", err)
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-o.writeCh:
		case <-lc.CloseSignal:
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*utils.KVWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-o.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.CloseSignal:
				goto closedCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-o.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

// writeRequests is called serially by only one goroutine.
func (o *OctopusDB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}
	err := o.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := o.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		o.Lock()
		o.updateHead(b.Ptrs)
		o.Unlock()
	}
	done(nil)
	return nil
}
func (o *OctopusDB) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if o.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			entry.Meta = entry.Meta &^ utils.BitValuePointer
		} else {
			entry.Meta = entry.Meta | utils.BitValuePointer
			entry.Value = b.Ptrs[i].Encode()
		}
		o.lsm.Set(entry)
	}
	return nil
}

// 结构体
type flushTask struct {
	mt           *utils.SkipList
	vptr         *utils.ValuePtr
	dropPrefixes [][]byte
}

func (o *OctopusDB) pushHead(ft flushTask) error {
	// Ensure we never push a zero valued head pointer.
	if ft.vptr.IsZero() {
		return errors.New("Head should not be zero")
	}

	fmt.Printf("Storing value log head: %+v\n", ft.vptr)
	val := ft.vptr.Encode()

	// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
	// commits.
	headTs := utils.KeyWithTs(head, uint64(time.Now().Unix()/1e9))
	ft.mt.Add(&utils.Entry{
		Key:   headTs,
		Value: val,
	})
	return nil
}
