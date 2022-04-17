package lsm

import (
	"fmt"
	"testing"
)

func TestLevelManager_pickCompactLevels(t *testing.T) {
	arr := make([]string, 3)
	arr[0], arr[1], arr[2] = "1", "2", "3"
	out := arr[:0]
	s := arr[:]
	fmt.Println(s)
	out = append(out, "4")
	out = append(out, "5")
	out = append(out, "6")
	arr = out
	fmt.Println(out)
	fmt.Println(&out, &arr)

	s1 := make([]int, 3)
	s1[0], s1[1], s1[2] = 1, 2, 3
	s2 := make([]int, 0)
	s2 = append(s2, 4)
	s2 = s1
	fmt.Println(s1, s2)
}
