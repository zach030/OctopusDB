package lsm

import (
	coreCache "github.com/zach030/OctopusDB/kv/utils/cache"
)

type cache struct {
	indexs *coreCache.Cache
	blocks *coreCache.Cache
}

const defaultCacheSize = 1024

func NewCache(cfg *Config) *cache {
	return &cache{
		indexs: coreCache.NewCache(defaultCacheSize),
		blocks: coreCache.NewCache(defaultCacheSize),
	}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}

func (c *cache) close() error {
	return nil
}
