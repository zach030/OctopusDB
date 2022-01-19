package lsm

import (
	"sort"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

type levelHandler struct {
	level   int
	tables  []*table
	manager *LevelManager
}

func (h *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if h.level == 0 {
		return h.SearchL0(key)
	}
	return h.SearchLN(key)
}

func (h *levelHandler) SearchL0(key []byte) (*utils.Entry, error) {
	// todo 调用table中的查询
	for _, t := range h.tables {
		t.Search()
	}
	return nil, nil
}

func (h *levelHandler) SearchLN(key []byte) (*utils.Entry, error) {
	return nil, nil
}

func (h *levelHandler) Sort() {
	if h.level == 0 {
		// 如果是第0级，还没有compact过，key可能有重叠，因此只按fid排序
		sort.Slice(h.tables, func(i, j int) bool {
			return h.tables[i].fid < h.tables[j].fid
		})
	} else {
		sort.Slice(h.tables, func(i, j int) bool {
			return utils.CompareKeys(h.tables[i].sst.MinKey(), h.tables[j].sst.MinKey()) < 0
		})
	}
}
