package file

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func Test_mmapReader_Read(t *testing.T) {
	data := []byte("hello")
	var offset = 0
	sz := binary.BigEndian.Uint32(data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(data) {
		return
	}
	res := data[start:next]
	fmt.Println(string(res))
}
