package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)

	//Put & Get
	entry1 := NewEntry([]byte("Key1"), []byte("Val1"))
	assert.Nil(t, list.Add(entry1))
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte("Key2"), []byte("Val2"))
	assert.Nil(t, list.Add(entry2))
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte("noexist")))

	//Update a entry
	entry2_new := NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, list.Add(entry2_new))
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(1000)
	key, val := "", ""
	maxTime := 1000000
	// todo skiplist benchmark need to update
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry)
		assert.Equal(b, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(1000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(t, l.Add(NewEntry(key(i), key(i))))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(t, key(i), v.Value)
				return
			}
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
			assert.Nil(b, l.Add(NewEntry(key(i), key(i))))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
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
	e1 := &Entry{Key: []byte("k1"), Value: []byte("v1")}
	e2 := &Entry{Key: []byte("k1"), Value: []byte("v1")}
	err := sl.Add(e1)
	require.NoError(t, err)
	err = sl.Add(e2)
	require.NoError(t, err)
	t.Run("update", func(t *testing.T) {
		e3 := &Entry{Key: []byte("k1"), Value: []byte("v1+1")}
		err := sl.Add(e3)
		require.NoError(t, err)
		ne := sl.Search([]byte("k1"))
		require.Equal(t, "v1+1", string(ne.Value))
	})
}

func TestSkipListSearch(t *testing.T) {
	sl := NewSkipList(1000)
	e1 := &Entry{Key: []byte("k1"), Value: []byte("v1")}
	e2 := &Entry{Key: []byte("k2"), Value: []byte("v2")}
	err := sl.Add(e1)
	require.NoError(t, err)
	err = sl.Add(e2)
	require.NoError(t, err)
	t.Run("search", func(t *testing.T) {
		ne1 := sl.Search([]byte("k1"))
		require.Equal(t, "v1", string(ne1.Value))
		ne2 := sl.Search([]byte("k2"))
		require.Equal(t, "v2", string(ne2.Value))
	})
}

func TestSkipListErr(t *testing.T) {
	sl := NewSkipList(1000)
	e1 := &Entry{Key: []byte("k1"), Value: []byte("v1")}
	e2 := &Entry{Key: []byte("k2"), Value: []byte("v2")}
	err := sl.Add(e1)
	require.NoError(t, err)
	err = sl.Add(e2)
	require.NoError(t, err)

	t.Run("key not exist", func(t *testing.T) {
		key := "not-exist"
		require.Nil(t, sl.Search([]byte(key)))
	})
}

func BenchmarkSkipList_AddSearch(b *testing.B) {
	sl := NewSkipList(1000)
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%v", i))
		value := []byte(fmt.Sprintf("value-%v", i))
		entry := &Entry{Key: key, Value: value}
		err := sl.Add(entry)
		require.Nil(b, err)
		e := sl.Search(key)
		require.Equal(b, value, e.Value)
	}
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList(1000)
	e1 := &Entry{Key: []byte("k1"), Value: []byte("v1")}
	e2 := &Entry{Key: []byte("k2"), Value: []byte("v2")}
	fmt.Println(sl.Add(e1))
	fmt.Println(sl.Add(e2))
	iter := sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		fmt.Println("key:", string(it.Entry().Key), ", value:", string(it.Entry().Value))
	}
}
