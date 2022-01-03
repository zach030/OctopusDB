package vlog

import "github.com/zach030/OctopusDB/internal/kv/utils"

type VLog struct {
}

type VLogOption struct {
}

func NewVLog(opt *VLogOption) *VLog {
	return &VLog{}
}

func (v *VLog) StartGC() {
	// todo vlog垃圾回收
}

func (v *VLog) Set(entry *utils.Entry) error {
	return nil
}

func (v *VLog) Get(entry *utils.Entry) (*utils.Entry, error) {
	// valuePtr := utils.ValuePtrDecode(entry.Value)
	return nil, nil
}
