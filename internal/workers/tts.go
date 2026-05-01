// Package workers tts.go contains worker implementations for text to speech
// Text to speech
// Returned bytes are audio data
package workers

import (
	"context"
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// TTS struct for implementing worker
// Contains 'tts' endpoint for make audio from text
type TTS struct {
	// Name of the worker
	Name   string
	log    *zap.Logger
	upg    websocket.Upgrader
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTTS creates a new TTS worker
func NewTTS(log *zap.Logger) *TTS {
	ctx, cancel := context.WithCancel(context.Background())
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &TTS{
		Name:   "TTS",
		log:    log,
		upg:    upg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// GetName returns the name of the worker
// Used for logging
func (t *TTS) GetName() string {
	return t.Name
}

// Register the worker endpoints on the http.ServeMux
func (t *TTS) Register(m *http.ServeMux) {
	m.HandleFunc("/tts", t.TTS)
}

// Close cancels the context
// Cancel context is used for shutdown WS connections
func (t *TTS) Close(ctx context.Context) {
	t.cancel()
}

// TTS makes audio from text
// Use WebSockets for streaming
// Returned bytes are audio data
func (t *TTS) TTS(w http.ResponseWriter, r *http.Request) {
	t.log.Info("TTS request")

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
