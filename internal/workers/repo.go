// Package workers contains workers interface.
package workers

import (
	"context"
	"net/http"
)

// Worker is an interface for workers.
// You can use it for custom endpoints.
type Worker interface {
	GetName() string
	Register(m *http.ServeMux)
	Close(ctx context.Context)
}
