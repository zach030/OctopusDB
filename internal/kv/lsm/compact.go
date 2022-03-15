package lsm

import (
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

// 压缩目标
type targets struct {
	baseLevel int     // 合并到的目标层
	targetSz  []int64 // 每层预期的总字节数
	fileSz    []int64 // 每层文件的预期大小
}

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

// compactDef 一次合并任务
type compactDef struct {
	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
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
		compactorId:  id,
		t:            p.t,
		p:            p,
		thisLevel:    l.levels[level],
		dropPrefixes: p.dropPrefixes,
	}

	if level == 0 {
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
	return true
}

// compactTables
func (l *LevelManager) compactTables(task *compactDef) bool {
	return true
}

func (l *LevelManager) pickCompactLevels() (prios []compactionPriority) {
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
	addPriority(0, l.CalcL0Score())
	for i := 1; i < len(l.levels); i++ {
		// 处于压缩状态的sst 不能计算在内
		// todo 获取正在压缩状态的sst文件大小
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
