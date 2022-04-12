package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte("Val1"))
	list.Add(entry1)
	vs := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte("Val2"))
	list.Add(entry2)
	vs = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte(RandString(10))).Value)

	//Update a entry
	entry2_new := NewEntry([]byte(RandString(10)), []byte("Val1+1"))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = RandString(10), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		list.Add(entry)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(t, key(i), v.Value)
			return

			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(1)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v.Value != nil {
				require.EqualValues(b, key(i), v.Value)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListAdd(t *testing.T) {
	sl := NewSkipList(1000)
	t.Run("update", func(t *testing.T) {
		key := RandString(10)
		e3 := &Entry{Key: []byte(key), Value: []byte("v1+1")}
		sl.Add(e3)
		ne := sl.Search([]byte(key))
		require.Equal(t, "v1+1", string(ne.Value))
	})
}

func TestSkipListSearch(t *testing.T) {
	sl := NewSkipList(1000)
	k1, k2 := RandString(10), RandString(10)
	e1 := &Entry{Key: []byte(k1), Value: []byte("v1")}
	e2 := &Entry{Key: []byte(k2), Value: []byte("v2")}
	sl.Add(e1)
	sl.Add(e2)
	t.Run("search", func(t *testing.T) {
		ne1 := sl.Search([]byte(k1))
		require.Equal(t, "v1", string(ne1.Value))
		ne2 := sl.Search([]byte(k2))
		require.Equal(t, "v2", string(ne2.Value))
	})
}

func TestSkipListErr(t *testing.T) {
	sl := NewSkipList(1000)
	k1, k2 := RandString(10), RandString(10)
	e1 := &Entry{Key: []byte(k1), Value: []byte("v1")}
	e2 := &Entry{Key: []byte(k2), Value: []byte("v2")}
	sl.Add(e1)
	sl.Add(e2)

	t.Run("key not exist", func(t *testing.T) {
		key := "not-exist-123456"
		require.Nil(t, sl.Search([]byte(key)).Value)
	})
}

func BenchmarkSkipList_AddSearch(b *testing.B) {
	sl := NewSkipList(100000000)
	for i := 0; i < b.N; i++ {
		key, val := RandString(10), fmt.Sprintf("Val%d", i)
		entry := &Entry{Key: []byte(key), Value: []byte(val)}
		sl.Add(entry)
		e := sl.Search([]byte(key))
		require.Equal(b, []byte(val), e.Value)
	}
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList(1000)
	k1, k2 := RandString(10), RandString(10)
	e1 := &Entry{Key: []byte(k1), Value: []byte("v1")}
	e2 := &Entry{Key: []byte(k2), Value: []byte("v2")}
	sl.Add(e1)
	sl.Add(e2)
	iter := sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		fmt.Println("key:", string(it.Entry().Key), ", value:", string(it.Entry().Value))
	}
}
