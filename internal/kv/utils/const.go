package utils

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold = 1024

	ManifestFileName   = "manifest"
	ReManifestFileName = "remanifest"
)

var (
	MagicText    = [4]byte{'O', 'C', 'T', 'O'}
	MagicVersion = uint32(1)
)
