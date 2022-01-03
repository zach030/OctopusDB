package kv

import (
	"github.com/zach030/OctopusDB/internal/kv/lsm"
	"github.com/zach030/OctopusDB/internal/kv/utils"
	"github.com/zach030/OctopusDB/internal/kv/vlog"
)

type API interface {
	Set(data *utils.Entry) error
	Get(key []byte) (*utils.Entry, error)
	Del(key []byte) error
	NewIterator(opt *Options) utils.Iterator
	Info() *stat
	Close() error
}

type OctopusDB struct {
	lsm  *lsm.LSM
	vlog *vlog.VLog
	opt  *Options
	stat *stat
}

func Open(opt *Options) *OctopusDB {
	db := &OctopusDB{opt: opt}
	db.lsm = lsm.NewLSM(&lsm.Config{
		WorkDir:            opt.WorkDir,
		MemTableSize:       opt.MemTableSize,
		SSTableMaxSz:       opt.SSTableMaxSz,
		BlockSize:          4 * 1024,
		BloomFalsePositive: 0.01,
	})
	db.stat = newStat()
	db.vlog = vlog.NewVLog(&vlog.VLogOption{})
	go db.lsm.StartCompaction()
	go db.vlog.StartGC()
	go db.stat.StartStat()
	return db
}

func (o *OctopusDB) Set(data *utils.Entry) error {
	// 1. 判断value大小
	var valuePtr *utils.ValuePtr
	if utils.ValueSize(data.Value) > o.opt.ValueThreshold {
		// 2. 如果大value，写入vlog
		valuePtr = utils.NewValuePtr(data)
		if err := o.vlog.Set(data); err != nil {
			return err
		}
	}
	if valuePtr != nil {

	}
	return o.lsm.Set(data)
	// 3. 将记录写入lsm
}

func (o *OctopusDB) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)
	// 1. 先从lsm拿key
	// 2. 如果存了vlog，再取value
	if entry, err = o.lsm.Get(key); err == nil {
		return entry, nil
	}
	// 判断是否存vlog
	if entry, err = o.vlog.Get(entry); err == nil {
		return entry, nil
	}
	return nil, nil
}

func (o *OctopusDB) Del(key []byte) error {
	return o.Set(&utils.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (o *OctopusDB) NewIterator(opt *Options) utils.Iterator {
	return nil
}

func (o *OctopusDB) Info() *stat {
	return o.stat
}

func (o *OctopusDB) Close() error {
	return nil
}
