// Package workers contains workers interface.
package workers

import (
	"context"
	"net/http"
)

const (
	defaultLength = 512
	defaultDicts  = "dicts/"
)

// Worker is an interface for workers.
// You can use it for custom endpoints.
type Worker interface {
	GetName() string
	Register(m *http.ServeMux)
	Close(ctx context.Context)
}
