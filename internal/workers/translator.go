// Package workers translator.go contains worker implementations for translator
// Translator text from one language to another
// Returned text is in the target language
package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"unsafe"

	etrl "github.com/Votline/EasyTranslate"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Translator struct for implementing worker
// Contains 'translate' endpoint for translating text
type Translator struct {
	// Name of the worker
	Name string

	// Preferred language. Used when no syntax 'from to' in first message
	prefLang string
	log      *zap.Logger
	upg      websocket.Upgrader
	trl      *etrl.EasyTranslate
}

// NewTranslator creates a new Translate worker
func NewTranslator(log *zap.Logger) *Translator {
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	translator := etrl.NewEasyTranslate(defaultDicts)

	return &Translator{
		Name: "Translator",
		log:  log,
		upg:  upg,
		trl:  translator,
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
	m.HandleFunc("/translate/setPrefferedLanguage", t.setPref)
}

// setPref sets preferred language
// Used as default language for translating text
func (t *Translator) setPref(w http.ResponseWriter, r *http.Request) {
	const op = "translator.setPref"

	var req struct {
		PrefLang string `json:"language"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.log.Error("Failed to decode request",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to decode request",
			http.StatusInternalServerError)
		return
	}

	t.prefLang = req.PrefLang

	t.log.Info("Set preferred language",
		zap.String("op", op),
		zap.String("prefLang", req.PrefLang))

	w.WriteHeader(http.StatusOK)
}

// Translate translates text from one language to another.
// First message must be 'from to'. Like: 'en ru'
// Use WebSockets for streaming.
// Returned text is in the target language.
func (t *Translator) Translate(w http.ResponseWriter, r *http.Request) {
	const op = "translator.translate"

	defer t.log.Debug("Leave", zap.String("op", op))

	lock := true

	if t.prefLang == "" {
		t.prefLang = "ru"
	}

	t.log.Info("Translate request",
		zap.String("op", op),
		zap.String("prefLang", t.prefLang))

	conn, err := t.upg.Upgrade(w, r, nil)
	if err != nil {
		t.log.Error("Failed to upgrade connection", zap.Error(err))
		http.Error(w, "Failed to upgrade connection",
			http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	t.log.Info("Upgraded connection",
		zap.String("op", op))

	inCom := etrl.NewRingBuffer(defaultLength)
	outCom := etrl.NewRingBuffer(defaultLength)

	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.log.Error("Failed to read message",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to read message",
			http.StatusInternalServerError)
		return
	}

	dict, err := t.prepareTranslator(msg)
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

	inCom.Write(msg)

	go func() {
		defer cancel()
		if err := t.trl.StartTranslate(ctx, inCom, dict, defaultLength, func(data []byte) {
			t.log.Debug("Received translated text",
				zap.String("op", op),
				zap.String("text", unsafe.String(unsafe.SliceData(data), len(data))))
			outCom.Write(data)
		}); err != nil {
			t.log.Error("Failed to start translate",
				zap.String("op", op),
				zap.Error(err))
			return
		}
	}()

	data := make([]byte, defaultLength)
	go func() {
		for {
			defer cancel()
			select {
			case <-ctx.Done():
				t.log.Info("Context done",
					zap.String("op", op))
				inCom.Close()
				return
			default:
				if !lock {
					_, msg, err = conn.ReadMessage()
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
				}
				cut := outCom.Read(data)
				if cut <= 0 {
					t.log.Error("Failed to read translated text",
						zap.String("op", op))
					return
				}

				err = conn.WriteMessage(websocket.TextMessage, data[:cut])
				if err != nil {
					t.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				t.log.Debug("Sent message",
					zap.String("op", op),
					zap.String("message", unsafe.String(unsafe.SliceData(data[:cut]), len(data[:cut]))))

				lock = false
			}
		}
	}()

	<-ctx.Done()
	t.log.Info("Context done", zap.String("op", op))
	inCom.Close()
}

// prepareTranslator prepares the easyTranslator package for translating
// First message must be 'from to'. Like: 'en ru'
func (t *Translator) prepareTranslator(msg []byte) (string, error) {
	const op = "translator.prepareTranslator"

	from, to, ok := bytes.Cut(msg, []byte(" "))
	if !ok {
		dict, err := t.prepareWithDetect(msg)
		if err != nil {
			return "", fmt.Errorf("%s: failed to prepare translator: %w", op, err)
		}
		return dict, nil
	}

	fromStr := unsafe.String(unsafe.SliceData(from), len(from))
	toStr := unsafe.String(unsafe.SliceData(to), len(to))

	dict, err := t.trl.EnsureTranslator(fromStr, toStr, defaultLength)
	if err != nil {
		dict, err = t.prepareWithDetect(msg)
		if err != nil {
			return "", fmt.Errorf("%s: failed to prepare translator: %w", op, err)
		}
	}

	return dict, nil
}

// prepareWithDetect is a fallback fucntion for prepareTranslator.
// It tries to detect language and prepare it.
// For target language it uses the preferred language.
func (t *Translator) prepareWithDetect(msg []byte) (string, error) {
	const op = "translator.prepareWithDetect"

	sourceStr := unsafe.String(unsafe.SliceData(msg), len(msg))

	if t.prefLang == "" {
		return "", fmt.Errorf("%s: no preferred language", op)
	}

	dict, err := t.trl.DetectWithEnsure(sourceStr, t.prefLang)
	if err != nil {
		return "", fmt.Errorf("%s: failed to detect language: %w", op, err)
	}

	return dict, nil
}
