package lsm

import (
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/zach030/OctopusDB/internal/kv/utils"
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
	randomDelay := time.NewTimer(time.Duration(rand.Intn(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:

	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			l.runOnce(id)
			//case <-lm.lsm.closer.CloseSignal:
			//	return
		}
	}
}

// runOnce 执行一次压缩任务
func (l *LevelManager) runOnce(id int) bool {
	// 生成本次压缩的任务
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

// run 执行一个优先级指定的合并任务
func (l *LevelManager) run(id int, p compactionPriority) bool {
	err := l.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 执行合并任务,将level层的某些文件合并到target层中
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
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = l.levels[level+1]
		}
		if !l.compactTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// 完成合并后 从合并状态中删除
	defer l.compactStatus.delete(cd) // Remove the ranges from compaction status.

	// 执行合并计划
	if err := l.runCompactDef(id, level, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

func (l *LevelManager) runCompactDef(id, level int, cd compactDef) (err error) {
	return
}

// compactTablesInL0 从l0--base的压缩，如果失败，尝试l0--l0的压缩
func (l *LevelManager) compactTablesFromL0(task *compactDef) bool {
	if l.fillTablesL0ToLbase(task) {
		return true
	}
	return l.fillTablesL0ToL0(task)
}

// compactTables
func (l *LevelManager) compactTables(task *compactDef) bool {
	return true
}

// fillTablesL0ToLbase 从l0压缩到l-base，获取l0层最大压缩区间，获取lbase的压缩区间，再检查是否有冲突的合并区间
func (l *LevelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		panic("next level not be 0")
	}
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		log.Printf("no need to do compact ,adjusted is between 0-1")
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
	for i := len(l.levels) - 1; i > 0; i++ {
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
		if utils.CompareKeys(t.sst.MinKey(), min) < 0 {
			min = t.sst.MinKey()
		}
		if utils.CompareKeys(t.sst.MaxKey(), max) > 0 {
			max = t.sst.MaxKey()
		}
	}
	return keyRange{
		left:  utils.KeyWithTs(min, math.MaxUint64),
		right: utils.KeyWithTs(max, 0),
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
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	//  [k-min,k-max]
	//              [d-min,d-max]
	if utils.CompareKeys(dst.left, r.right) > 0 {
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
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}
