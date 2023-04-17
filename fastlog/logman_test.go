package fastlog_test

import (
	"testing"
	"time"

	"github.com/lemon-mint/ubiquitous-logs/fastlog"
)

var data [128]byte

func BenchmarkLogManager(b *testing.B) {
	lm := fastlog.NewLogManager("test", ".logs", 4096*16)
	defer lm.Close()
	b.SetBytes(128)
	for i := 0; i < b.N; i++ {
		lm.Write(time.Now().UnixNano(), data[:])
	}
}
