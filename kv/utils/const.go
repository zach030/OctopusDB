package utils

import "math"

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold             = 1024
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	ManifestFileName                  = "manifest"
	ReManifestFileName                = "remanifest"

	datasyncFileFlag = 0x0
	// 基于可变长编码,其最可能的编码
	MaxHeaderSize            = 21
	VlogHeaderSize           = 0
	MaxVlogFileSize   uint32 = math.MaxUint32
	Mi                int64  = 1 << 20
	KVWriteChCapacity        = 1000
)

// meta
const (
	BitDelete       byte = 1 << 0 // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.
)

var (
	MagicText    = [4]byte{'O', 'C', 'T', 'O'}
	MagicVersion = uint32(1)
)
