package lsm

import (
	"fmt"
	"testing"
)

var (
	opt = &Config{
		WorkDir:             "../data",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       3,
	}
)

func TestLSM_openMemTable(t *testing.T) {
	lsm := NewLSM(opt)

	mem := lsm.openMemTable(uint64(1))
	e := mem.Get([]byte("key"))
	fmt.Println(string(e.Entry().Value))
}
