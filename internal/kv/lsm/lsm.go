package lsm

type LSM struct {
	memTable *MemTable
	imMemTable []*MemTable
}

func NewLSM()*LSM{
	return &LSM{}
}