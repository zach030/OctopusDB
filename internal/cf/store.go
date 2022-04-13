package cf

import (
	"github.com/zach030/OctopusDB/internal/kv"
	"github.com/zach030/OctopusDB/internal/kv/utils"
)

var engine kv.Engine

func init() {
	engine = NewOctopusStoreEngine()
}

func NewOctopusStoreEngine() *kv.OctopusDB {
	return kv.Open(kv.NewDefaultOptions())
}

func Get(key []byte) ([]byte, error) {
	item, err := engine.Get(key)
	if err == utils.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	val := make([]byte, len(item.Value))
	copy(val, item.Value)
	return val, nil
}
