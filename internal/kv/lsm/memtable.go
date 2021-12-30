package lsm

import "github.com/zach030/OctopusDB/internal/kv/utils"

type MemTable struct {
	skipList *utils.SkipList
}
