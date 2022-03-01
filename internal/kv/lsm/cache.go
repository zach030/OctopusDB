package lsm

import coreCache "github.com/zach030/OctopusDB/internal/kv/utils/cache"

type cache struct {
	indexs *coreCache.Cache
	blocks *coreCache.Cache
}

const defaultCacheSize = 1024

func NewCache(cfg *Config) *cache {
	var size = cfg.CacheSize
	if size == 0 {
		size = defaultCacheSize
	}
	return &cache{
		indexs: coreCache.NewCache(size),
		blocks: coreCache.NewCache(size),
	}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}

func (c *cache) close() error {
	return nil
}
