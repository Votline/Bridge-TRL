// Package workers contains worker implementations for translate
// Translate text from one language to another
// Returned text is in the target language
package workers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"unsafe"

	etrl "github.com/Votline/EasyTranslate"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	defaultLength = 4096
	defaultDicts  = "dicts/"
)

// Translator struct for implementing worker
// Contains 'translate' endpoint for translating text
type Translator struct {
	Name   string
	log    *zap.Logger
	upg    websocket.Upgrader
	trl    *etrl.EasyTranslate
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTranslate creates a new Translate worker
func NewTranslate(log *zap.Logger) *Translator {
	ctx, cancel := context.WithCancel(context.Background())
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	translator := etrl.NewEasyTranslate(defaultDicts)

	return &Translator{
		Name:   "Translate",
		log:    log,
		upg:    upg,
		trl:    translator,
		ctx:    ctx,
		cancel: cancel,
	}
}

// GetName returns the name of the worker
// Used for logging
func (t *Translator) GetName() string {
	return t.Name
}

// Register the worker endpoints on the http.ServeMux
func (t *Translator) Register(m *http.ServeMux) {
	m.HandleFunc("/translate", t.Translate)
}

// Close cancels the context
// Cancel context is used for shutdown WS connections
func (t *Translator) Close(ctx context.Context) {
	t.cancel()
}

// Translate translates text from one language to another.
// First message must be 'from to'. Like: 'en ru'
// Use WebSockets for streaming.
// Returned text is in the target language.
func (t *Translator) Translate(w http.ResponseWriter, r *http.Request) {
	const op = "translator.translate"

	t.log.Info("Translate request",
		zap.String("op", op))

	conn, err := t.upg.Upgrade(w, r, nil)
	if err != nil {
		t.log.Error("Failed to upgrade connection", zap.Error(err))
		http.Error(w, "Failed to upgrade connection",
			http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	t.log.Info("Upgraded connection",
		zap.String("op", op))

	inCom := etrl.NewRingBuffer(defaultLength)
	outCom := etrl.NewRingBuffer(defaultLength)

	dict, err := t.prepareTranslator(conn)
	if err != nil {
		t.log.Error("Failed to prepare translator",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to prepare translator.\nError: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	t.log.Info("Detected language",
		zap.String("op", op),
		zap.String("dict", dict))

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := t.trl.StartTranslate(t.ctx, inCom, dict, func(data []byte) {
			t.log.Debug("Received translated text",
				zap.String("op", op),
				zap.String("text", unsafe.String(unsafe.SliceData(data), len(data))))
			outCom.Write(data)
		}); err != nil {
			t.log.Error("Failed to start translate",
				zap.String("op", op),
				zap.Error(err))
			t.cancel()
		}
	})

	data := make([]byte, defaultLength)
	for {
		select {
		case <-t.ctx.Done():
			t.log.Info("Context done",
				zap.String("op", op))
			inCom.Close()
			return
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				t.log.Error("Failed to read message",
					zap.String("op", op),
					zap.Error(err))
				return
			}

			t.log.Debug("Received message",
				zap.String("op", op),
				zap.String("message", unsafe.String(unsafe.SliceData(msg), len(msg))))

			inCom.Write(msg)
			cut := outCom.Read(data)
			if cut <= 0 {
				t.log.Error("Failed to read translated text",
					zap.String("op", op))
				return
			}
			data = data[:cut]

			err = conn.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				t.log.Error("Failed to write message",
					zap.String("op", op),
					zap.Error(err))
				return
			}

			t.log.Debug("Sent message",
				zap.String("op", op),
				zap.String("message", string(data)))
		}
	}
}

// prepareTranslator prepares the easyTranslator package for translating
// First message must be 'from to'. Like: 'en ru'
func (t *Translator) prepareTranslator(conn *websocket.Conn) (string, error) {
	const op = "translator.prepareTranslator"

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return "", fmt.Errorf("%s: read first message: %w", op, err)
	}

	from, to, ok := bytes.Cut(msg, []byte(" "))
	if !ok {
		return "", fmt.Errorf("%s: failed to cut message. Needed format: 'source to'", op)
	}

	fromStr := unsafe.String(unsafe.SliceData(from), len(from))
	toStr := unsafe.String(unsafe.SliceData(to), len(to))

	if err := t.trl.EnsureTranslator(fromStr, toStr, defaultLength); err != nil {
		return "", fmt.Errorf("%s: ensure translator: %w", op, err)
	}

	dict := strings.ToLower(fromStr + "_" + toStr)

	return dict, nil
}
