// Package workers stt.go contains worker implementations for speech to text
// Speech to text
// Returned text is in the source language
package workers

import (
	"context"
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// STT struct for implementing worker
// Contains 'stt' endpoint for speech to text
type STT struct {
	// Name of the worker
	Name   string
	log    *zap.Logger
	upg    websocket.Upgrader
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSTT creates a new STT worker
func NewSTT(log *zap.Logger) *STT {
	ctx, cancel := context.WithCancel(context.Background())
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &STT{
		Name:   "STT",
		log:    log,
		upg:    upg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// GetName returns the name of the worker
// Used for logging
func (t *STT) GetName() string {
	return t.Name
}

// Register the worker endpoints on the http.ServeMux
func (t *STT) Register(m *http.ServeMux) {
	m.HandleFunc("/stt", t.STT)
}

// Close cancels the context
// Cancel context is used for shutdown WS connections
func (t *STT) Close(ctx context.Context) {
	t.cancel()
}

// STT speech to text
// Use WebSockets for streaming
// Returned text is in the source language
func (t *STT) STT(w http.ResponseWriter, r *http.Request) {
	t.log.Info("STT request")

	conn, err := t.upg.Upgrade(w, r, nil)
	if err != nil {
		t.log.Error("Failed to upgrade connection", zap.Error(err))
		http.Error(w, "Failed to upgrade connection",
			http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	t.log.Info("Upgraded connection")

	for {
		select {
		case <-t.ctx.Done():
			t.log.Info("Context done")
			return
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				t.log.Error("Failed to read message", zap.Error(err))
				return
			}

			t.log.Info("Received message",
				zap.String("message", string(msg)))

			err = conn.WriteMessage(websocket.TextMessage, []byte("pong"))
			if err != nil {
				t.log.Error("Failed to write message", zap.Error(err))
				return
			}
		}
	}
}
