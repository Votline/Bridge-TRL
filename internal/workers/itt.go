package workers

import (
	"context"
	"net/http"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/otiai10/gosseract"
	"go.uber.org/zap"
)

// ITT struct for implementing image to text worker
type ITT struct {
	// Name of the worker
	Name string

	// rec is a recognizer for image to text
	rec *gosseract.Client

	log *zap.Logger
	upg websocket.Upgrader
}

// NewITT creates a new ITT worker
func NewITT(log *zap.Logger) *ITT {
	const op = "workeri.NewITT"

	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	rec := gosseract.NewClient()
	rec.SetLanguage("rus", "eng")

	return &ITT{
		Name: "ITT",
		rec:  rec,
		log:  log,
		upg:  upg,
	}
}

// GetName returns the name of the worker
// Used for logging
func (i *ITT) GetName() string {
	return i.Name
}

// Register the worker endpoints on the http.ServeMux
func (i *ITT) Register(m *http.ServeMux) {
	m.HandleFunc("/itt", i.ITT)
}

func (i *ITT) Close(ctx context.Context) {
}

func (i *ITT) ITT(w http.ResponseWriter, r *http.Request) {
	const op = "workeri.ITT"

	defer i.log.Debug("Leave", zap.String("op", op))

	i.log.Info("ITT request",
		zap.String("op", op))

	conn, err := i.upg.Upgrade(w, r, nil)
	if err != nil {
		i.log.Error("Failed to upgrade connection",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to upgrade connection",
			http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	i.log.Info("Upgraded connection",
		zap.String("op", op))

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				i.log.Info("Context done", zap.String("op", op))
				return
			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					i.log.Error("Failed to read message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				i.log.Info("Received message",
					zap.String("op", op),
					zap.Int("message len", len(msg)))

				i.rec.SetImageFromBytes(msg)
				res, err := i.rec.Text()
				if err != nil {
					i.log.Error("Failed to recognize text",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				resBytes := unsafe.Slice(unsafe.StringData(res), len(res))
				i.log.Info("Send text",
					zap.String("op", op),
					zap.Int("text len", len(resBytes)))

				if err := conn.WriteMessage(websocket.TextMessage, resBytes); err != nil {
					i.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		i.log.Info("Context done", zap.String("op", op))
		return
	}
}
