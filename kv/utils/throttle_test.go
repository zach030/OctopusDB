package utils

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/log"
)

func TestNewThrottle(t *testing.T) {
	th := NewThrottle(3)
	for i := 0; i < 10; i++ {
		if err := th.Do(); err != nil {
			log.Error("do failed,err:", err)
			return
		}
		go func(i int) {
			fmt.Println("curr is:", i)
			defer th.Done(nil)
		}(i)
	}

	th.Finish()
	log.Info("all done")
}
