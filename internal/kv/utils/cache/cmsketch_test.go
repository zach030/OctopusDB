package cache

import (
	"fmt"
	"testing"
)

func Test_newCmRow(t *testing.T) {
	cr := newCmRow(4)
	for i := 0; i < 3; i++ {
		cr.increment(uint64(3))
		val := cr.get(uint64(3))
		fmt.Println("num-3-cnt:", val)

		cr.increment(uint64(2))
		val = cr.get(uint64(2))
		fmt.Println("num-2-cnt:", val)
	}
}

func Test_newCmSketch(t *testing.T) {
	cms := newCmSketch(8)
	for i := 0; i < 7; i++ {
		for j := 0; j < 3; j++ {
			cms.Increment(uint64(i))
			val := cms.Estimate(uint64(i))
			fmt.Println("num-", i, "-count:", val)
		}
	}
}
