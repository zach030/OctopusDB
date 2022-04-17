package kv

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/zach030/OctopusDB/kv/file"
	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/pkg/errors"
)

func (vlog *valueLog) runGC(discardRatio float64, head *utils2.ValuePtr) error {
	select {
	case vlog.garbageCh <- struct{}{}:
		// Pick a log file for GC.
		defer func() {
			// 通过一个channel来控制一次仅运行一个GC任务
			<-vlog.garbageCh
		}()

		var err error
		files := vlog.pickLog(head)
		if len(files) == 0 {
			return utils2.ErrNoRewrite
		}
		tried := make(map[uint32]bool)
		for _, lf := range files {
			//消重一下,防止随机策略和统计策略返回同一个fid
			if _, done := tried[lf.FID]; done {
				continue
			}
			tried[lf.FID] = true
			if err = vlog.doRunGC(lf, discardRatio); err == nil {
				return nil
			}
		}
		return err
	default:
		return utils2.ErrRejected
	}
}

func (vlog *valueLog) doRunGC(lf *file.LogFile, discardRatio float64) (err error) {
	// 退出的时候把统计的discard清空
	defer func() {
		if err == nil {
			vlog.lfDiscardStats.Lock()
			delete(vlog.lfDiscardStats.m, lf.FID)
			vlog.lfDiscardStats.Unlock()
		}
	}()
	s := &sampler{
		lf:            lf,
		countRatio:    0.01, // 1% of num entries.
		sizeRatio:     0.1,  // 10% of the file as window.
		fromBeginning: false,
	}

	if _, err = vlog.sample(s, discardRatio); err != nil {
		return err
	}

	if err = vlog.rewrite(lf); err != nil {
		return err
	}
	return nil
}

//重写
func (vlog *valueLog) rewrite(f *file.LogFile) error {
	vlog.filesLock.RLock()
	maxFid := vlog.maxFid
	vlog.filesLock.RUnlock()
	if f.FID >= maxFid {
		panic(fmt.Errorf("fid to move: %d. Current max fid: %d", f.FID, maxFid))
	}

	wb := make([]*utils2.Entry, 0, 1000)
	var size int64

	var count, moved int
	fe := func(e *utils2.Entry) error {
		count++
		if count%100000 == 0 {
			fmt.Printf("Processing entry %d\n", count)
		}

		vs, err := vlog.db.lsm.Get(e.Key)
		if err != nil {
			return err
		}
		if utils2.DiscardEntry(e, vs) {
			return nil
		}

		if len(vs.Value) == 0 {
			return errors.Errorf("Empty value: %+v", vs)
		}
		var vp utils2.ValuePtr
		vp.Decode(vs.Value)

		if vp.Fid > f.FID {
			return nil
		}
		if vp.Offset > e.Offset {
			return nil
		}
		// 如果从lsm和vlog的同一个位置读取带entry则重新写回，也有可能读取到旧的
		if vp.Fid == f.FID && vp.Offset == e.Offset {
			moved++
			// This new entry only contains the key, and a pointer to the value.
			ne := new(utils2.Entry)
			ne.Meta = 0 // Remove all bits. Different keyspace doesn't need these bits.
			ne.ExpiresAt = e.ExpiresAt
			ne.Key = append([]byte{}, e.Key...)
			ne.Value = append([]byte{}, e.Value...)
			es := int64(ne.EstimateSize(vlog.db.opt.ValueLogFileSize))
			// Consider size of value as well while considering the total size
			// of the batch. There have been reports of high memory usage in
			// rewrite because we don't consider the value size. See #1292.
			es += int64(len(e.Value))

			// Ensure length and size of wb is within transaction limits.
			if int64(len(wb)+1) >= vlog.opt.MaxBatchCount ||
				size+es >= vlog.opt.MaxBatchSize {
				if err := vlog.db.batchSet(wb); err != nil {
					return err
				}
				size = 0
				wb = wb[:0]
			}
			wb = append(wb, ne)
			size += es
		}
		return nil
	}

	_, err := f.Iterate(0, func(e *utils2.Entry, vp *utils2.ValuePtr) error {
		return fe(e)
	})
	if err != nil {
		return err
	}

	batchSize := 1024
	var loops int
	for i := 0; i < len(wb); {
		loops++
		if batchSize == 0 {
			return utils2.ErrNoRewrite
		}
		end := i + batchSize
		if end > len(wb) {
			end = len(wb)
		}
		if err := vlog.db.batchSet(wb[i:end]); err != nil {
			if err == utils2.ErrTxnTooBig {
				// Decrease the batch size to half.
				batchSize = batchSize / 2
				continue
			}
			return err
		}
		i += batchSize
	}
	var deleteFileNow bool
	// Entries written to LSM. Remove the older file now.
	{
		vlog.filesLock.Lock()
		// Just a sanity-check.
		if _, ok := vlog.filesMap[f.FID]; !ok {
			vlog.filesLock.Unlock()
			return errors.Errorf("Unable to find fid: %d", f.FID)
		}
		if vlog.iteratorCount() == 0 {
			delete(vlog.filesMap, f.FID)
			//deleteFileNow = true
		} else {
			vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, f.FID)
		}
		vlog.filesLock.Unlock()
	}

	if deleteFileNow {
		if err := vlog.deleteLogFile(f); err != nil {
			return err
		}
	}

	return nil
}

func (vlog *valueLog) pickLog(head *utils2.ValuePtr) (files []*file.LogFile) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	fids := vlog.sortedFids()
	switch {
	// 只有一个log文件那不需要进行GC了
	case len(fids) <= 1:
		return nil
		// fid 是0说明是初次启动，更不需要gc了
		// TODO 先不处理head
		// case head.Fid == 0:
		// 	return nil
	}

	// 创建一个候选对象
	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}
	// 加锁遍历fids，选择小于等于head fid的列表中discard统计最大的那个log文件
	// discard 就是在compact过程中统计的可丢弃key的数量
	vlog.lfDiscardStats.RLock()
	for _, fid := range fids {
		if fid >= head.Fid {
			break
		}
		if vlog.lfDiscardStats.m[fid] > candidate.discard {
			candidate.fid = fid
			candidate.discard = vlog.lfDiscardStats.m[fid]
		}
	}
	vlog.lfDiscardStats.RUnlock()

	// 说明这是一个有效候选
	if candidate.fid != math.MaxUint32 { // Found a candidate
		files = append(files, vlog.filesMap[candidate.fid])
	}

	// 再补充一种随机选择的fid，比如应对初次执行时discard的统计不充分的情况
	var idxHead int
	for i, fid := range fids {
		if fid == head.Fid {
			idxHead = i
			break
		}
	}
	if idxHead == 0 { // Not found or first file
		idxHead = 1 // 开始对
	}
	idx := rand.Intn(idxHead) // Don’t include head.Fid. We pick a random file before it.
	if idx > 0 {
		idx = rand.Intn(idx + 1) // Another level of rand to favor smaller fids.
	}
	files = append(files, vlog.filesMap[fids[idx]])
	return files
}

type sampler struct {
	lf            *file.LogFile
	sizeRatio     float64
	countRatio    float64
	fromBeginning bool
}

func (vlog *valueLog) sample(samp *sampler, discardRatio float64) (*reason, error) {
	sizePercent := samp.sizeRatio
	countPercent := samp.countRatio
	fileSize := samp.lf.Size()
	// Set up the sampling winxdow sizes.
	sizeWindow := float64(fileSize) * sizePercent
	sizeWindowM := sizeWindow / (1 << 20) // in MBs.
	countWindow := int(float64(vlog.opt.ValueLogMaxEntries) * countPercent)

	var skipFirstM float64
	var err error
	// Skip data only if fromBeginning is set to false. Pick a random start point.
	if !samp.fromBeginning {
		// Pick a random start point for the log.
		skipFirstM = float64(rand.Int63n(fileSize)) // Pick a random starting location.
		skipFirstM -= sizeWindow                    // Avoid hitting EOF by moving back by window.
		skipFirstM /= float64(utils2.Mi)            // Convert to MBs.
	}
	var skipped float64

	var r reason
	start := time.Now()
	var numIterations int
	// 重放遍历vlog文件
	_, err = samp.lf.Iterate(0, func(e *utils2.Entry, vp *utils2.ValuePtr) error {
		numIterations++
		esz := float64(vp.Len) / (1 << 20) // in MBs.
		if skipped < skipFirstM {
			skipped += esz
			return nil
		}
		// Sample until we reach the window sizes or exceed 10 seconds.
		if r.count > countWindow {
			return utils2.ErrStop
		}
		if r.total > sizeWindowM {
			return utils2.ErrStop
		}
		if time.Since(start) > 10*time.Second {
			return utils2.ErrStop
		}
		r.total += esz
		r.count++

		entry, err := vlog.db.Get(e.Key)
		if err != nil {
			return err
		}
		if utils2.DiscardEntry(e, entry) {
			r.discard += esz
			return nil
		}

		// Value is still present in value log.
		if len(entry.Value) <= 0 {
			panic(fmt.Errorf("len(entry.Value) <= 0"))
		}
		vp.Decode(entry.Value)

		if vp.Fid > samp.lf.FID {
			// Value is present in a later log. Discard.
			r.discard += esz
			return nil
		}
		if vp.Offset > e.Offset {
			// Value is present in a later offset, but in the same log.
			r.discard += esz
			return nil
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	fmt.Printf("Fid: %d. Skipped: %5.2fMB Num iterations: %d. Data status=%+v\n",
		samp.lf.FID, skipped, numIterations, r)
	// If we couldn't sample at least a 1000 KV pairs or at least 75% of the window size,
	// and what we can discard is below the threshold, we should skip the rewrite.
	if (r.count < countWindow && r.total < sizeWindowM*0.75) || r.discard < discardRatio*r.total {
		fmt.Printf("Skipping GC on fid: %d", samp.lf.FID)
		return nil, utils2.ErrNoRewrite
	}
	return &r, nil
}

func (vlog *valueLog) waitOnGC(lc *utils2.Closer) {
	defer lc.Done()

	<-lc.CloseSignal // Wait for lc to be closed.

	// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
	// the channel of size 1.
	vlog.garbageCh <- struct{}{}
}

type reason struct {
	total   float64
	discard float64
	count   int
}

func (vlog *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&vlog.numActiveIterators))
}

// TODO 在迭代器close时，需要调用此函数，关闭已经被判定需要移除的logfile
func (vlog *valueLog) decrIteratorCount() error {
	num := atomic.AddInt32(&vlog.numActiveIterators, -1)
	if num != 0 {
		return nil
	}

	vlog.filesLock.Lock()
	lfs := make([]*file.LogFile, 0, len(vlog.filesToBeDeleted))
	for _, id := range vlog.filesToBeDeleted {
		lfs = append(lfs, vlog.filesMap[id])
		delete(vlog.filesMap, id)
	}
	vlog.filesToBeDeleted = nil
	vlog.filesLock.Unlock()

	for _, lf := range lfs {
		if err := vlog.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) deleteLogFile(lf *file.LogFile) error {
	if lf == nil {
		return nil
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	lf.Close()
	return os.Remove(lf.FileName())
}
