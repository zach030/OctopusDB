package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4 // 与哈希函数的个数相同，用来分四批计数
)

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64 // 掩码
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}

	numCounters = next2Power(numCounters)
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}

	return sketch
}

// Increment 进行计数
func (s *cmSketch) Increment(hashed uint64) {
	for i := range s.rows {
		// 通过与随机数异或运算，可以把数据分散到不同的计数器，这样会导致数据偏大
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

// Estimate 在管理的多个计数中选出最小的
func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}

	return int64(min)
}

// Reset halves all counter values.
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

// next2Power 求最接近x的二次幂
// todo example: x=74: 1001010，最接近的72: 1000000
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

type cmRow []byte

// 每四个bit作为一个counter,1 byte=8 bit= 2 counter

func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

// increment 对n的计数进行累加
func (r cmRow) increment(n uint64) {
	i := n / 2
	// n&1 判断是奇偶，奇数末尾为1，&1结果为1，*4=4；偶数末尾为0，&1结果为0，*4=0
	// 在1byte=8bit中，奇数占用0-4位的计数器，偶数占用5-8位的计数器
	// example: cmRow=[0000,0000;0000,0000],可供4个数计数,分别对应1,0,3,2
	s := (n & 1) * 4
	// 偶数：s=0; 奇数:s=4
	v := (r[i] >> s) & 0x0f // 这一步是求当前的计数，因为奇数在高四位，需要右移，与0x1111进行与运算，得到真实的计数
	// 4bit，最大计数值为15
	if v < 15 {
		r[i] += 1 << s
	}
}

// reset 数值减半
func (r cmRow) reset() {
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77 // 0111 0111
	}
}

// clear 清空
func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

func (r cmRow) string() string {
	s := ""
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		s += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	s = s[:len(s)-1]
	return s
}
