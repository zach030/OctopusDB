package utils

import "math"

type BloomFilter struct {
	Filter []byte
}

func NewBloomFilter(keys []uint32, bitPerKey int) BloomFilter {
	return BloomFilter{Filter: appendFilter(keys, bitPerKey)}
}

func (b BloomFilter) MayContain(key []byte) bool {
	return b.mayContain(Hash(key))
}

func (b BloomFilter) mayContain(h uint32) bool {
	if len(b.Filter) < 2 {
		return false
	}
	k := b.Filter[len(b.Filter)-1]
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (len(b.Filter) - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if b.Filter[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// BitsPerKey 根据计算公式，在给定假阳性false positive和确定数据量下
func BitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(math.Ln2, 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// CalcHashNum 计算出最佳的hash函数个数
func CalcHashNum(bitsPerKey int) uint32 {
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := CalcHashNum(bitsPerKey)
	nBits := len(keys) * bitsPerKey
	if nBits < 64 {
		nBits = 64
	}
	// 使用1bit来判断：0 or 1， 1byte = 8bit
	nBytes := (nBits + 7) / 8
	// 构造nbit大小的filter
	filter := make([]byte, nBytes+1)
	for _, key := range keys {
		delta := key>>17 | key<<15
		for j := uint32(0); j < k; j++ {
			// 使用k个哈希函数
			bitPos := key % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			key += delta
		}
	}
	filter[nBytes] = uint8(k)
	return filter
}

func Hash(b []byte) uint32 {
	// todo implement hash func
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return uint32(0)
}
