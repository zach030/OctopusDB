package utils

type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

func NewValuePtr(entry *Entry) *ValuePtr {
	return &ValuePtr{}
}

func IsValuePtr(entry *Entry) bool {
	return false
}

func ValuePtrCodec(vp *ValuePtr) []byte {
	return []byte{}
}
