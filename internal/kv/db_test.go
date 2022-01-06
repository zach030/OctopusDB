package kv

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zach030/OctopusDB/internal/kv/utils"
)

func TestBasicSetGet(t *testing.T) {
	opt := NewDefaultOptions()
	db := Open(opt)
	defer db.Close()
	entry := utils.NewEntry([]byte("key"), []byte("value"))
	if err := db.Set(entry); err != nil {
		log.Fatal(err)
	}
	e, err := db.Get([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, []byte("value"), e.Value)
}
