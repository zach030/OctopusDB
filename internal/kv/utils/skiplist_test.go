package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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
