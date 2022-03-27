package kv

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

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

func TestAPI(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	// 写入
	for i := 0; i < 50; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

	for i := 0; i < 40; i++ {
		key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		if err := db.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 10; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

}
