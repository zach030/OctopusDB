package lsm

import (
	"log"
	"math/rand"
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
	if p.level >= l.cfg.MaxLevelNum {
		panic("compact level exceed max level")
	}

	return nil
}

func (l *LevelManager) pickCompactLevels() (prios []compactionPriority) {
	return nil
}

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
