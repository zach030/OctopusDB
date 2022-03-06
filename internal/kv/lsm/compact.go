package lsm

import (
	"math/rand"
	"time"
)

// 压缩目标
type targets struct {
	baseLevel int // 压缩的目标层
	targetSz  []int64
	fileSz    []int64
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

func (l *LevelManager) runOnce(id int) bool {
	return true
}
