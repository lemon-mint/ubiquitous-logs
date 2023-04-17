package main

import (
	"fmt"
	"time"

	"github.com/lemon-mint/ubiquitous-logs/fastlog"
)

func main() {
	lm := fastlog.NewLogManager("test", ".logs", 4096*16)
	defer lm.Close()
	for i := 0; i < 10000; i++ {
		lm.Write(time.Now().UnixNano(), []byte(fmt.Sprintf("test %d", i)))
	}
}
