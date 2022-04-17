package cache

import (
	"fmt"

	lfu "github.com/dgryski/go-tinylfu"
)

type Cache struct {
	t *lfu.T
}

func NewCache(size int) *Cache {
	return &Cache{t: lfu.New(size, size)}
}

func (c *Cache) Set(key interface{}, value interface{}) {
	c.t.Add(fmt.Sprintf("%v", key), value)
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	return c.t.Get(fmt.Sprintf("%v", key))
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	// todo implement cache delete
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	return nil, false
}
