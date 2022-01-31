package lsm

import "github.com/zach030/OctopusDB/internal/kv/utils"

type tableBuilder struct {
}

func newTableBuilder(cfg *Config) *tableBuilder {
	return &tableBuilder{}
}

func (t *tableBuilder) add(e *utils.Entry, isStale bool) {
	// todo write to disk
}
