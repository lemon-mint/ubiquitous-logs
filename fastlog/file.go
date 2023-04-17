package fastlog

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/valyala/bytebufferpool"
)

var pageSize = os.Getpagesize()
var pool bytebufferpool.Pool

type file struct {
	index      *os.File
	data       *os.File
	dataOffset int64

	indexBuffer *bytebufferpool.ByteBuffer
	dataBuffer  *bytebufferpool.ByteBuffer
}

func newFile(index, data *os.File) (*file, error) {
	var err error

	f := &file{data: data, index: index}
	f.dataOffset, err = f.data.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	f.indexBuffer = pool.Get()
	f.dataBuffer = pool.Get()

	if len(f.indexBuffer.B) < pageSize {
		f.indexBuffer.B = make([]byte, pageSize)
	}
	if len(f.dataBuffer.B) < pageSize {
		f.dataBuffer.B = make([]byte, pageSize)
	}
	f.indexBuffer.B = f.indexBuffer.B[:0]
	f.dataBuffer.B = f.dataBuffer.B[:0]

	return f, nil
}

func (f *file) Close() error {
	err0 := f.Flush()
	err1 := f.index.Close()
	err2 := f.data.Close()
	if err0 != nil {
		return err0
	}
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}

	pool.Put(f.indexBuffer)
	pool.Put(f.dataBuffer)
	return nil
}

func (f *file) Flush() error {
	if len(f.indexBuffer.B) > 0 {
		if _, err := f.index.Write(f.indexBuffer.B); err != nil {
			return err
		}
		f.indexBuffer.B = f.indexBuffer.B[:0]
	}

	if len(f.dataBuffer.B) > 0 {
		if _, err := f.data.Write(f.dataBuffer.B); err != nil {
			return err
		}
		f.dataBuffer.B = f.dataBuffer.B[:0]
	}

	return nil
}

func (f *file) Write(ts int64, b []byte) error {
	if len(f.indexBuffer.B)+16 > pageSize {
		if _, err := f.index.Write(f.indexBuffer.B); err != nil {
			return err
		}
		f.indexBuffer.B = f.indexBuffer.B[:0]
	}

	if len(f.dataBuffer.B)+len(b) > pageSize {
		if _, err := f.data.Write(f.dataBuffer.B); err != nil {
			return err
		}
		f.dataBuffer.B = f.dataBuffer.B[:0]
	}

	f.indexBuffer.B = f.indexBuffer.B[:len(f.indexBuffer.B)+16]
	binary.LittleEndian.PutUint64(f.indexBuffer.B[len(f.indexBuffer.B)-16:], uint64(ts))
	binary.LittleEndian.PutUint64(f.indexBuffer.B[len(f.indexBuffer.B)-8:], uint64(f.dataOffset))

	f.dataBuffer.B = append(f.dataBuffer.B, b...)
	f.dataOffset += int64(len(b))

	return nil
}

func (f *file) Size() int64 {
	return f.dataOffset
}
