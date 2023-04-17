package fastlog

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/xid"
)

type LogManager struct {
	namespace string
	path      string
	maxsize   int64

	mu          sync.Mutex
	currentFile *file
}

func NewLogManager(nameSpace, path string, maxSize int64) *LogManager {
	os.MkdirAll(path, 0755)
	return &LogManager{
		namespace: nameSpace,
		path:      path,
		maxsize:   maxSize,
	}
}

func (lm *LogManager) Lock() {
	lm.mu.Lock()
}

func (lm *LogManager) Unlock() {
	lm.mu.Unlock()
}

func (lm *LogManager) Write(ts int64, b []byte) error {
	if lm.currentFile == nil || lm.currentFile.Size() >= lm.maxsize {
		if lm.currentFile != nil {
			err := lm.currentFile.Close()
			if err != nil {
				return err
			}
		}

		id := xid.New()
		prefix := filepath.Join(lm.path, lm.namespace+"_"+id.String()+".")
		index, err := os.Create(prefix + "fbzidx")
		if err != nil {
			return err
		}
		data, err := os.Create(prefix + "fbzlog")
		if err != nil {
			return err
		}
		lm.currentFile, err = newFile(index, data)
		if err != nil {
			return err
		}
	}

	return lm.currentFile.Write(ts, b)
}

func (lm *LogManager) Flush() error {
	if lm.currentFile != nil {
		return lm.currentFile.Flush()
	}
	return nil
}

func (lm *LogManager) Close() error {
	if lm.currentFile != nil {
		return lm.currentFile.Close()
	}
	return nil
}
