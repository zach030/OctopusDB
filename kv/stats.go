package kv

type stat struct {
	num int
}

func newStat() *stat {
	return &stat{}
}

func (s *stat) StartStat() {
	// todo real-time stat
}

func (s *stat) Close() error {
	return nil
}
