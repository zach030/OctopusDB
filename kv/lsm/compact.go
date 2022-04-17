package lsm

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zach030/OctopusDB/kv/pb"
	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
)

type compactionPriority struct {
	level        int     // 压缩来源层
	score        float64 // 来源层得分
	adjusted     float64 // 来源层调整得分
	dropPrefixes [][]byte
	t            targets
}

// 压缩目标
type targets struct {
	baseLevel int     // 合并到的目标层
	targetSz  []int64 // 每层预期的总字节数
	fileSz    []int64 // 每层文件的预期大小
}

// compactDef 一次合并任务
type compactDef struct {
	compactorID int                // 执行compact的任务id
	t           targets            // 压缩目标
	p           compactionPriority //
	thisLevel   *levelHandler      // lx层
	nextLevel   *levelHandler      // ly层 目标压缩层

	top    []*table
	bottom []*table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func (l *LevelManager) runCompacter(id int) {
	defer l.lsm.closer.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Intn(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-l.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			l.runOnce(id)
		case <-l.lsm.closer.CloseSignal:
			return
		}
	}
}

// runOnce 执行一次压缩任务
func (l *LevelManager) runOnce(id int) bool {
	log.Info("[Compact] compactor id:", id, " is running")
	// 生成本次压缩的任务,一组压缩比超过1的任务
	prios := l.pickCompactLevels()
	if id == 0 {
		// 如果0号协程则压缩l0层
		prios = improvePriorityOfL0(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {

		} else if p.adjusted < 1.0 {
			break
		}
		if l.run(id, p) {
			return true
		}
	}
	return true
}

// run 执行一个优先级指定的合并任务，已知从lx--ly，需要确定具体的压缩区间
func (l *LevelManager) run(id int, p compactionPriority) bool {
	err := l.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils2.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Infof("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 执行合并任务,将level层的某些文件合并到target层中,生成具体的压缩任务:计算压缩区间top,bottom
func (l *LevelManager) doCompact(id int, p compactionPriority) error {
	level := p.level
	if p.level >= l.cfg.MaxLevelNum {
		panic("compact level exceed max level")
	}
	if p.t.baseLevel == 0 {
		// 兜底，如果目标压缩层为0
		p.t = l.levelTarget()
	}
	cd := compactDef{
		compactorID:  id,
		t:            p.t,
		p:            p,
		thisLevel:    l.levels[level],
		dropPrefixes: p.dropPrefixes,
	}

	if level == 0 {
		// 从l0--l-base层的压缩
		cd.nextLevel = l.levels[p.t.baseLevel]
		if !l.compactTablesFromL0(&cd) {
			return utils2.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = l.levels[level+1]
		}
		if !l.compactTables(&cd) {
			return utils2.ErrFillTables
		}
	}
	// 完成合并后 从合并状态中删除
	defer l.compactStatus.delete(cd) // Remove the ranges from compaction status.

	// 执行合并计划
	if err := l.runCompactDef(id, level, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Infof("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Infof("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// runCompactDef 执行压缩计划
func (l *LevelManager) runCompactDef(id, level int, cd compactDef) (err error) {
	// todo 具体执行合并任务
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel
	// 分解cd
	if thisLevel != nextLevel {
		// 分解任务
		l.addSplits(&cd)
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}
	// 具体执行压缩，返回压缩后的新tables，作为nextLevel新的tables
	newTbls, decr, err := l.compactBuildTables(level, cd)
	if err != nil {
		return err
	}
	defer func() {
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	// 更新manifest
	modifies := addManifestModifySet(&cd, newTbls)
	if err := l.manifestFile.AddChanges(modifies.Modifies); err != nil {
		return err
	}
	// 更新nextLevel的table列表
	if err := nextLevel.ReplaceTables(cd.bottom, newTbls); err != nil {
		return err
	}
	// 更新thisLevel的table列表
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	// 输出日志，记录本次合并耗时
	from := append(tablesToString(cd.top), tablesToString(cd.bottom)...)
	to := tablesToString(newTbls)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bottom),
			len(newTbls), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// compactBuildTables 核心的压缩实现，将thisLevel.top与nextLevel.bottom进行合并压缩，生成newTables
func (l *LevelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {
	topTables := cd.top
	botTables := cd.bottom
	iterOpt := &utils2.Options{
		IsAsc: true,
	}
	//numTables := int64(len(topTables) + len(botTables))

	newIterator := func() []utils2.Iterator {
		// Create iterators across all the tables involved first.
		var iters []utils2.Iterator
		switch {
		case lev == 0:
			iters = append(iters, reversedIterators(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = []utils2.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// 开始并行执行压缩过程，流式处理提高效率
	res := make(chan *table, 3)
	// 限定并发协程数为split+8
	inflightBuilders := utils2.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		// 开启一个协程去处理子压缩
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := NewMergeIterator(newIterator(), false)
			defer it.Close()
			// 核心实现子压缩
			l.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	// mapreduce的方式收集table的句柄
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			// subcompact在做子压缩，向chan发送数据，这里遍历channel读数据
			newTables = append(newTables, t)
		}
	}()

	// 在这里等待所有的压缩过程完成
	err := inflightBuilders.Finish()
	// channel 资源回收
	close(res)
	// 等待所有的builder刷到磁盘
	wg.Wait()

	if err == nil {
		// 同步刷盘，保证数据一定落盘
		err = utils2.SyncDir(l.cfg.WorkDir)
	}

	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils2.CompareKeys(newTables[i].sst.MaxKey(), newTables[j].sst.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func (l *LevelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *l.lsm.cfg.DiscardStatsCh <- discardStats:
	default:
	}
}

// 真正执行并行压缩的子压缩文件
func (l *LevelManager) subcompact(it utils2.Iterator, kr keyRange, cd compactDef, inflightBuilders *utils2.Throttle, res chan<- *table) {
	var lastKey []byte
	// 更新 discardStats
	discardStats := make(map[uint32]int64)
	defer func() {
		l.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *utils2.Entry) {
		if e.Meta&utils2.BitValuePointer > 0 {
			var vp utils2.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := utils.ParseTs(key)
			isExpired := isDeletedOrExpired(0, it.Item().Entry().ExpiresAt)
			if !utils2.IsSameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils2.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = utils2.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils2.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// TODO 这里要区分值的指针
			// 判断是否是过期内容，是的话就删除
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	} // End of function: addKeys

	//如果 key range left还存在 则seek到这里 说明遍历中途停止了
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		//
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.right) > 0 && utils2.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 拼装table创建的参数
		// TODO 这里可能要大改，对open table的参数复制一份opt
		builder := newTableBuilderWithSSTSize(l.cfg, cd.t.fileSz[cd.nextLevel.levelNum])

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			log.Info("builder for level:", cd.nextLevel, " will finish")
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// 充分发挥 ssd的并行 写入特性
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&l.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			// TODO 这里的sst文件需要根据level大小变化
			sstName := utils2.SSTFullFileName(l.cfg.WorkDir, newFID)
			tbl = openTable(l, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

func reversedIterators(th []*table, opt *utils2.Options) []utils2.Iterator {
	out := make([]utils2.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

func addManifestModifySet(cd *compactDef, newTables []*table) pb.ManifestModifies {
	changes := make([]*pb.ManifestModify, 0)
	for _, tbl := range newTables {
		changes = append(changes, &pb.ManifestModify{
			Id:    tbl.fid,
			Op:    pb.ManifestModify_CREATE,
			Level: uint32(cd.nextLevel.levelNum),
		})
	}
	for _, tbl := range append(cd.top, cd.bottom...) {
		changes = append(changes, &pb.ManifestModify{
			Id: tbl.fid,
			Op: pb.ManifestModify_DELETE,
		})
	}
	return pb.ManifestModifies{Modifies: changes}
}

func (l *LevelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// 分成5组
	group := int(math.Ceil(float64(len(cd.bottom)) / 5.0))
	if group < 3 {
		group = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = utils2.Copy(right)
		cd.splits = append(cd.splits, skr)
		// left右移，将skr分片
		skr.left = skr.right
	}

	for i, t := range cd.bottom {
		// last entry in bottom table.
		if i == len(cd.bottom)-1 {
			addRange([]byte{})
			return
		}
		if i%group == group-1 {
			// 设置最大值为右区间
			right := utils2.KeyWithTs(utils2.ParseKey(t.sst.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// compactTablesInL0 从l0--base的压缩，如果失败，尝试l0--l0的压缩
func (l *LevelManager) compactTablesFromL0(task *compactDef) bool {
	if l.fillTablesL0ToLbase(task) {
		return true
	}
	return l.fillTablesL0ToL0(task)
}

// compactTables 常规的从lx--ly的压缩
func (l *LevelManager) compactTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.levelNum)
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// 如果当前是最后一层，则执行max--max的压缩
	if cd.thisLevel.isLastLevel() {
		return l.compactMaxLevelTables(tables, cd)
	}
	l.sortWithVersion(tables, cd)
	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		if l.compactStatus.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			// 如果thisRange与正在合并的区间有重合，就跳过
			continue
		}
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bottom = make([]*table, right-left)
		copy(cd.bottom, cd.nextLevel.tables[left:right])

		if len(cd.bottom) == 0 {
			cd.bottom = []*table{}
			cd.nextRange = cd.thisRange
			if !l.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bottom...)
		// 判断目标层级待压缩区间是否有冲突
		if l.compactStatus.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		if !l.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

// compactMaxLevelTables 从max--max的同级压缩
func (l *LevelManager) compactMaxLevelTables(tables []*table, cd *compactDef) bool {
	return true
}

// sortWithVersion 根据数据版本排序，把旧版本放在前面，优先合并
func (l *LevelManager) sortWithVersion(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].sst.Index().MaxVersion < tables[j].sst.Index().MaxVersion
	})
}

// fillTablesL0ToLbase 从l0压缩到l-base，获取l0层最大压缩区间，获取lbase的压缩区间，再检查是否有冲突的合并区间
func (l *LevelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		panic("next level not be 0")
	}
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		log.Infof("no need to do compact ,adjusted is between 0-1")
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()
	// 获取this，next中压缩源与目标层的待压缩table，分别是top和bottom
	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}
	out := make([]*table, 0)
	kr := keyRange{}
	// 在top中找到最大区间
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// 因为按递增的顺序，如果发现有不重叠的，后面就不用判断
			break
		}
	}
	// 获取this层的合并后的区间
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	bottom := make([]*table, right-left)
	copy(bottom, cd.nextLevel.tables[left:right])

	if len(bottom) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(bottom...)
	}

	// 记录sst进入合并状态
	return l.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 l0--l0的压缩
func (l *LevelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorID != 0 {
		return false
	}
	cd.nextLevel = l.levels[0]
	cd.nextRange = keyRange{}
	now := time.Now()
	top := cd.thisLevel.tables
	out := make([]*table, 0)
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			continue
		}
		if _, ok := l.compactStatus.tables[t.fid]; ok {
			continue
		}
		out = append(out, t)
	}
	return true
}

func (l *LevelManager) pickCompactLevels() (prios []compactionPriority) {
	// 拿到本次合并任务的目标层，和当前每层限制的数值
	t := l.levelTarget()
	// 记录compact优先级func
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}
	// 计算每一层的压缩优先级信息
	addPriority(0, l.CalcL0Score())
	for i := 1; i < len(l.levels); i++ {
		// 处于压缩状态的sst 不能计算在内
		delSize := l.compactStatus.delSize(i)
		level := l.levels[i]
		sz := level.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	if len(l.levels) != len(prios) {
		panic("Compact pick level err,len(l.levels) != len(prios)")
	}
	// 调整得分
	var prevLevel int
	for level := t.baseLevel; level < len(l.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// 避免过大的得分
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}
	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按优先级排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// CalcL0Score 计算l0层的得分：sst文件数/L0最多文件数
func (l *LevelManager) CalcL0Score() float64 {
	return float64(l.levels[0].tableNums()) / float64(l.cfg.NumLevelZeroTables)
}

// 对于非L0层的层，score的计算方式为，level总SST的大小除以该level对应的要做compact的阈值
// 对于L0层的score,计算方式为max(L0层所有文件的格式/level0_file_num_compaction_trigger, L0层所有SST文件总大小-max_bytes_for_level_base)

// 将压缩l0层的任务移到数组开始
func improvePriorityOfL0(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, prio := range prios {
		if prio.level == 0 {
			idx = i
			break
		}
	}
	if idx > 0 {
		ret := append([]compactionPriority{}, prios[idx])
		ret = append(ret, prios[:idx]...)
		ret = append(ret, prios[idx+1:]...)
		return ret
	}
	return prios
}

// levelTarget 构建本次合并的目标
func (l *LevelManager) levelTarget() targets {
	var adjust = func(size int64) int64 {
		// 调整baseLevelSize
		if size < l.cfg.BaseLevelSize {
			return l.cfg.BaseLevelSize
		}
		return size
	}
	t := targets{
		targetSz: make([]int64, len(l.levels)),
		fileSz:   make([]int64, len(l.levels)),
	}
	// 从最后一层开始遍历
	dbSize := l.lastLevelSize()
	for i := len(l.levels) - 1; i > 0; i-- {
		targetSize := adjust(dbSize)
		t.targetSz[i] = targetSize
		if t.baseLevel == 0 && targetSize <= l.cfg.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(l.cfg.LevelSizeMultiplier)
	}

	tsz := l.cfg.BaseTableSize
	for i := 0; i < len(l.levels); i++ {
		if i == 0 {
			// l0选择memtable的size作为文件的尺寸
			t.fileSz[i] = l.cfg.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(l.cfg.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}
	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := l.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (l *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < l.cfg.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

func (cs *compactStatus) delete(cd compactDef) {

}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

type thisAndNextLevelRLocked struct{}

// compareAndAdd 比较本次要合并的区间与正在执行的任务是否冲突，记录合并状态
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]
	// 如果本次合并的区间与正在合并的任务区间冲突，则取消本次任务
	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bottom...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// overlapsWith 判断当前level是否有正在合并的区间与我们此次待合并区间重合
func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

// getKeyRange 给定一定的sst，找出合并的最大区间
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	min := tables[0].sst.MinKey()
	max := tables[0].sst.MaxKey()
	for _, t := range tables {
		if utils2.CompareKeys(t.sst.MinKey(), min) < 0 {
			min = t.sst.MinKey()
		}
		if utils2.CompareKeys(t.sst.MaxKey(), max) > 0 {
			max = t.sst.MaxKey()
		}
	}
	return keyRange{
		left:  utils2.KeyWithTs(min, math.MaxUint64),
		right: utils2.KeyWithTs(max, 0),
	}
}

// overlapsWith if k is overlap with dst
// [k-min, k-max], [dst-min, dst-max]
func (r keyRange) overlapsWith(dst keyRange) bool {
	if r.isEmpty() {
		return true
	}
	if dst.isEmpty() {
		return false
	}
	// todo what's inf? infinite means overlap?
	if r.inf || dst.inf {
		return true
	}
	//              [k-min        k-max]
	// [d-min,d-max]
	if utils2.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	//  [k-min,k-max]
	//              [d-min,d-max]
	if utils2.CompareKeys(dst.left, r.right) > 0 {
		return false
	}
	return true
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils2.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils2.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

// 判断是否过期 是可删除
func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}
