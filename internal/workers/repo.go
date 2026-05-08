// Package workers contains workers interface.
package workers

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"unsafe"

	rb "btrl/internal/ringbuffer"
)

const (
	defaultLength = 512
	defaultRBLen  = defaultLength
	defAudioLen   = 4096
	defaultDicts  = "dicts/"
)

// bufPool is a pool for buffers
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, defaultLength)
		return &b
	},
}

var ringBufPool = sync.Pool{
	New: func() any {
		b := rb.NewRB[byte](defaultRBLen)
		return b
	},
}

var audioBufPool = sync.Pool{
	New: func() any {
		b := make([]float32, defAudioLen)
		return &b
	},
}

var int16AudioBufPool = sync.Pool{
	New: func() any {
		b := make([]int16, defAudioLen)
		return &b
	},
}

// Worker is an interface for workers.
// You can use it for custom endpoints.
type Worker interface {
	GetName() string
	Register(m *http.ServeMux)
	Close(ctx context.Context)
}

func findAPIPrefix(d string) bool {
	data := unsafe.Slice(unsafe.StringData(d), len(d))

	has := bytes.Index(data, []byte("http"))
	if has == -1 {
		has = bytes.Index(data, []byte("https"))
		if has == -1 {
			return false
		}
	}
	return true
}
