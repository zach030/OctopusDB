package file

import "github.com/zach030/OctopusDB/internal/kv/utils"

type Wal struct {
}

func OpenWalFile(opt *Option) *Wal {
	return &Wal{}
}

// todo implement wal add method

func (w *Wal) Add(entry *utils.Entry) error {
	return nil
}
