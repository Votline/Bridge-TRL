// Package workers contains workers interface.
package workers

import (
	"context"
	"net/http"
	"sync"
)

const (
	defaultLength = 512
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

var audioBufPool = sync.Pool{
	New: func() any {
		b := make([]float32, defAudioLen)
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
