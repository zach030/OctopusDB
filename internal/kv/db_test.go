package kv

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/common/log"

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
	t.Run("add", func(t *testing.T) {
		for i := 0; i < 5000; i++ {
			key, val := fmt.Sprintf("key%d%v", i, time.Now().UnixNano()), fmt.Sprintf("val%d", i)
			e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
			if err := db.Set(e); err != nil {
				t.Fatal(err)
			}
			// 查询
			//if _, err := db.Get([]byte(key)); err != nil {
			//	t.Fatal(err)
			//}
			//else {
			//	t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
			//}
		}
	})

	t.Run("delete", func(t *testing.T) {
		for i := 0; i < 40; i++ {
			key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
			if err := db.Del([]byte(key)); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("search", func(t *testing.T) {
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
	})
}

type User struct {
	ID        string
	Name      string
	Follower  []string
	Following []string
}

func TestGenerateRandomUserInfo(t *testing.T) {
	users := make([]User, 0)
	cnt := 100
	for i := 0; i < cnt; i++ {
		rand.Seed(time.Now().Unix())
		u := User{
			ID:        strconv.Itoa(i),
			Name:      fmt.Sprintf("user-%v", i),
			Follower:  getRandomFollower(time.Now().UnixNano()),
			Following: getRandomFollower(time.Now().UnixNano()),
		}
		users = append(users, u)
	}
	content, err := json.Marshal(&users)
	if err != nil {
		log.Error(err)
		return
	}
	ioutil.WriteFile("user.json", content, 0666)
}

func getRandomFollower(seed int64) []string {
	rand.Seed(seed)
	ret := make([]string, 0)
	cnt := rand.Intn(10)
	for i := 0; i < cnt; i++ {
		ret = append(ret, strconv.Itoa(rand.Intn(100)))
	}
	return ret
}

func TestUserInfoQuery(t *testing.T) {
	clearDir()
	bytes, err := ioutil.ReadFile("user.json")
	if err != nil {
		log.Error(err)
		return
	}
	users := make([]User, 0)
	json.Unmarshal(bytes, &users)

	db := Open(opt)
	defer db.Close()
	for _, user := range users {
		val, _ := json.Marshal(user)
		e := utils.NewEntry([]byte(user.ID), val).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < len(users); i++ {
		u := users[i]
		e, err := db.Get([]byte(u.ID))
		if err != nil {
			t.Fatal(err)
		}
		user := User{}
		err = json.Unmarshal(e.Value, &user)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, u.Name, user.Name)
		require.Equal(t, u.Follower, user.Follower)
		require.Equal(t, u.Following, user.Following)
	}
}
