package kv

import (
	"bytes"
	"testing"

	utils2 "github.com/zach030/OctopusDB/kv/utils"

	"github.com/stretchr/testify/require"
)

var (
	// 初始化opt
	opt1 = &Options{
		WorkDir:          "./data",
		SSTableMaxSz:     1 << 10,
		MemTableSize:     1 << 10,
		ValueLogFileSize: 1 << 20,
		ValueThreshold:   0,
		MaxBatchCount:    10,
		MaxBatchSize:     1 << 20,
	}
)

func TestVlogBase(t *testing.T) {
	// 清理目录
	clearDir()
	// 打开DB
	db := Open(opt1)
	defer db.Close()
	log := db.vlog
	var err error
	// 创建一个简单的kv entry对象
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, int64(len(val1)) >= db.opt.ValueThreshold)

	e1 := &utils2.Entry{
		Key:   []byte("samplekey"),
		Value: []byte(val1),
		Meta:  utils2.BitValuePointer,
	}
	e2 := &utils2.Entry{
		Key:   []byte("samplekeyb"),
		Value: []byte(val2),
		Meta:  utils2.BitValuePointer,
	}

	// 构建一个批量请求的request
	b := new(request)
	b.Entries = []*utils2.Entry{e1, e2}

	// 直接写入vlog中
	log.write([]*request{b})
	require.Len(t, b.Ptrs, 2)
	t.Logf("Pointer written: %+v %+v\n", b.Ptrs[0], b.Ptrs[1])

	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	buf1, lf1, err1 := log.readValueBytes(b.Ptrs[0])
	buf2, lf2, err2 := log.readValueBytes(b.Ptrs[1])
	require.NoError(t, err1)
	require.NoError(t, err2)
	// 关闭会调的锁
	cb := log.getUnlockCallback(lf1)
	if cb != nil {
		cb()
	}
	cb = log.getUnlockCallback(lf2)
	if cb != nil {
		cb()
	}
	e1, err = lf1.DecodeEntry(buf1, b.Ptrs[0].Offset)
	require.NoError(t, err)
	// 从vlog文件中通过指指针反序列化回 entry对象
	e2, err = lf1.DecodeEntry(buf2, b.Ptrs[1].Offset)
	require.NoError(t, err)

	// 比较entry对象是否相等
	readEntries := []utils2.Entry{*e1, *e2}
	require.EqualValues(t, []utils2.Entry{
		{
			Key:    []byte("samplekey"),
			Value:  []byte(val1),
			Meta:   utils2.BitValuePointer,
			Offset: b.Ptrs[0].Offset,
		},
		{
			Key:    []byte("samplekeyb"),
			Value:  []byte(val2),
			Meta:   utils2.BitValuePointer,
			Offset: b.Ptrs[1].Offset,
		},
	}, readEntries)
}

func TestValueGC(t *testing.T) {
	clearDir()
	opt.ValueLogFileSize = 1 << 20
	kv := Open(opt)
	defer kv.Close()
	sz := 32 << 10

	t.Run("single", func(t *testing.T) {
		e := newRandEntry(sz)
		key := make([]byte, len(e.Key))
		copy(key, e.Key)
		value := make([]byte, len(e.Value))
		copy(value, e.Value)
		require.NoError(t, kv.Set(e))
		item, err := kv.Get(key)
		require.NoError(t, err)
		require.True(t, bytes.Equal(utils2.ParseKey(item.Key), key), "key not equal: item:%s, entry:%s", utils2.ParseKey(item.Key), key)
		require.True(t, bytes.Equal(item.Value, value), "value not equal: item:%s, entry:%s", item.Value, value)
	})

	kvList := []*utils2.Entry{}
	for i := 0; i < 100; i++ {
		e := newRandEntry(sz)
		kvList = append(kvList, &utils2.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Meta:      e.Meta,
			ExpiresAt: e.ExpiresAt,
		})
		require.NoError(t, kv.Set(e))
	}
	kv.RunValueLogGC(0.9)
	for _, e := range kvList {
		item, err := kv.Get(e.Key)
		require.NoError(t, err)
		val := getItemValue(t, item)
		require.NotNil(t, val)
		require.True(t, bytes.Equal(utils2.ParseKey(item.Key), e.Key), "key not equal: item:%s, entry:%s", utils2.ParseKey(item.Key), e.Key)
		require.True(t, bytes.Equal(item.Value, e.Value), "value not equal: item:%s, entry:%s", item.Value, e.Value)
	}
}

func getItemValue(t *testing.T, item *utils2.Entry) (val []byte) {
	t.Helper()
	if item == nil {
		return nil
	}
	var v []byte
	v = append(v, item.Value...)
	if v == nil {
		return nil
	}
	return v
}
