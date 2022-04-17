package lsm

import (
	"bytes"
	"sort"
	"sync"

	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/pkg/errors"
)

// levelHandler 管理每一层的元数据
type levelHandler struct {
	sync.RWMutex
	level          int
	totalSize      int64
	totalStaleSize int64
	tables         []*table
	manager        *LevelManager
	levelNum       int
}

func (h *levelHandler) close() error {
	return nil
}

func (h *levelHandler) Get(key []byte) (*utils2.Entry, error) {
	if h.level == 0 {
		return h.SearchL0(key)
	}
	return h.SearchLN(key)
}

func (h *levelHandler) SearchL0(key []byte) (*utils2.Entry, error) {
	var version uint64
	for _, t := range h.tables {
		if entry, err := t.Search(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, errors.New("key not found")
}

func (h *levelHandler) SearchLN(key []byte) (*utils2.Entry, error) {
	tbl := h.getTable(key)
	if tbl == nil {
		return nil, errors.New("key not found")
	}
	var version uint64
	if entry, err := tbl.Search(key, &version); err == nil {
		return entry, nil
	}
	return nil, errors.New("key not found")
}

func (h *levelHandler) getTable(key []byte) *table {
	for i := 0; i < len(h.tables); i++ {
		if bytes.Compare(key, h.tables[i].sst.MinKey()) > -1 && bytes.Compare(key, h.tables[i].sst.MaxKey()) < 1 {
			return h.tables[i]
		}
	}
	return nil
}

func (h *levelHandler) Sort() {
	if h.level == 0 {
		// 如果是第0级，还没有compact过，key可能有重叠，因此只按fid排序
		sort.Slice(h.tables, func(i, j int) bool {
			return h.tables[i].fid < h.tables[j].fid
		})
	} else {
		sort.Slice(h.tables, func(i, j int) bool {
			return utils2.CompareKeys(h.tables[i].sst.MinKey(), h.tables[j].sst.MinKey()) < 0
		})
	}
}

// add table to this level
func (h *levelHandler) add(t *table) {
	h.Lock()
	defer h.Unlock()
	h.tables = append(h.tables, t)
}

// batchAdd batch add tables to this level
func (h *levelHandler) batchAdd(ts []*table) {
	h.Lock()
	defer h.Unlock()
	h.tables = append(h.tables, ts...)
}

func (h *levelHandler) getTotalSize() int64 {
	h.Lock()
	defer h.Unlock()
	return h.totalSize
}

func (h *levelHandler) addSize(t *table) {
	h.totalSize += t.Size()
	h.totalStaleSize += int64(t.StaleSize())
}

func (h *levelHandler) subtractSize(t *table) {
	h.totalSize -= t.Size()
	h.totalStaleSize -= int64(t.StaleSize())
}

func (h *levelHandler) tableNums() int {
	h.Lock()
	defer h.Unlock()
	return len(h.tables)
}

func (h *levelHandler) deleteTables(ts []*table) error {
	h.Lock()
	deleteMap := make(map[uint64]struct{})
	for _, t := range ts {
		deleteMap[t.fid] = struct{}{}
	}
	newTbls := make([]*table, 0)
	for _, t := range h.tables {
		if _, ok := deleteMap[t.fid]; !ok {
			newTbls = append(newTbls, t)
		} else {
			h.subtractSize(t)
		}
	}
	h.tables = newTbls
	h.Unlock()
	return decrRefs(ts)
}

func (h *levelHandler) isLastLevel() bool {
	return h.level == h.manager.cfg.MaxLevelNum-1
}

// ReplaceTables after compact, replace old tables with new tables
func (h *levelHandler) ReplaceTables(old, new []*table) error {
	h.Lock()
	// 从原来本层的tables中删除old
	// 将剩下的table与new组合成本层的新tables
	oldTables := make(map[uint64]struct{})
	for _, t := range old {
		oldTables[t.fid] = struct{}{}
	}
	newTables := make([]*table, 0)
	for _, t := range h.tables {
		if _, ok := oldTables[t.fid]; !ok {
			// 不在被删除的旧table中，仍然保存new
			newTables = append(newTables, t)
		} else {
			h.subtractSize(t)
		}
	}
	for _, t := range new {
		h.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}
	h.tables = newTables
	sort.Slice(h.tables, func(i, j int) bool {
		return utils2.CompareKeys(h.tables[i].sst.MinKey(), h.tables[j].sst.MinKey()) < 0
	})
	h.Unlock()
	return decrRefs(old)
}
