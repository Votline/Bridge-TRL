package workers

import (
	"context"
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type Translate struct {
	Name   string
	log    *zap.Logger
	upg    websocket.Upgrader
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTranslate(log *zap.Logger) *Translate {
	ctx, cancel := context.WithCancel(context.Background())
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &Translate{
		Name:   "Translate",
		log:    log,
		upg:    upg,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *Translate) GetName() string {
	return t.Name
}

func (t *Translate) Register(m *http.ServeMux) {
	m.HandleFunc("/translate", t.Translate)
}

func (t *Translate) Translate(w http.ResponseWriter, r *http.Request) {
	t.log.Info("Translate request")

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

func (t *Translate) Close(ctx context.Context) {
	t.cancel()
}
