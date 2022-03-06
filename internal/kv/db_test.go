package kv

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

var (
	opt = NewDefaultOptions()
)

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}

func TestClear(t *testing.T) {
	clearDir()
}

func TestBasicSetGet(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	e := utils.NewEntry([]byte("key"), []byte("value"))
	en, err := db.Get([]byte("key"))
	db.Set(e)
	for i := 0; i < 64; i++ {
		e := newRandEntry(16)
		db.Set(e)
	}
	en, err = db.Get([]byte("key"))
	fmt.Println(string(en.Value), err)
}

func newRandEntry(sz int) *utils.Entry {
	v := make([]byte, sz)
	rand.Read(v[:rand.Intn(sz)])
	e := utils.BuildEntry()
	e.Value = v
	return e
}
