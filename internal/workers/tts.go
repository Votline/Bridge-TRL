// Package workers tts.go contains worker implementations for text to speech
// Text to speech
// Returned bytes are audio data
package workers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	"unsafe"

	rb "btrl/internal/ringbuffer"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	ttsModeScript = -2
	ttsModeAPI    = -3
	channels      = 1
	sampleRate    = 24000
	duration      = 60
)

// TTS struct for implementing worker
// Contains 'tts' endpoint for make audio from text
type TTS struct {
	// Name of the worker
	Name string

	// call is URL or path to script, which makes audio from text
	// You can use offline ollama AI or any API
	call string

	// voiceID is ID of the voice
	// It may be required for API requests
	// In request body it must be "voice_id" with string value
	voiceID string

	// modelName is name of the AI model
	// It may be required for API requests
	// In request body it must be "model_name"
	modelName string

	// Mode is a mode of the worker
	// Can be: script or api
	// Script mode is used for calling script, which makes audio from text
	// API mode is used for calling API, which makes audio from text
	mode int

	// ReadTimeout is a timeout during which messages are collected
	// This is necessary in order not to send short messages, but whole sentences.
	// ReadTimeout only works if no point is found.
	// Default is 2 seconds
	readTimeout time.Duration

	// client is a client for send requests
	client *http.Client

	log *zap.Logger
	upg websocket.Upgrader
}

// NewTTS creates a new TTS worker
func NewTTS(log *zap.Logger) *TTS {
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &TTS{
		Name: "TTS",
		call: "",
		log:  log,
		upg:  upg,
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
	m.HandleFunc("/tts/setOptions", t.setOptions)
}

// setOptions sets options of the worker
// Used for setting call, voiceID and modelName
// Call is URL or path to script, which makes audio from text
// VoiceID is ID of the voice. 'voice_id' with string value in request
// ModelName is name of the AI model. 'model_name' in request
func (t *TTS) setOptions(w http.ResponseWriter, r *http.Request) {
	const op = "tts.setCall"

	var req struct {
		Call        string `json:"call"`
		VoiceID     string `json:"voice_id"`
		ModelName   string `json:"model_name"`
		ReadTimeout int    `json:"read_timeout"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.log.Error("Failed to decode request",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to decode request",
			http.StatusInternalServerError)
		return
	}

	t.call = req.Call
	t.voiceID = req.VoiceID
	t.modelName = req.ModelName

	t.mode = ttsModeAPI
	if ok := findAPIPrefix(t.call); !ok {
		t.mode = ttsModeScript
	}

	if req.ReadTimeout == 0 {
		req.ReadTimeout = 2
	}

	t.readTimeout = time.Duration(req.ReadTimeout) * time.Second

	client := &http.Client{
		Timeout: t.readTimeout,
	}
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	t.client = client

	t.log.Info("Set options",
		zap.String("op", op),
		zap.String("call", req.Call),
		zap.String("voiceID", req.VoiceID),
		zap.String("modelName", req.ModelName),
		zap.String("readTimeout", fmt.Sprintf("%d", req.ReadTimeout)))

	w.WriteHeader(http.StatusOK)
}

// defaultOptions set default options
// 'Artemiy' voice is used by default
// Write to /dev/stdout is used by default
func (t *TTS) defaultOptions() {
	const op = "tts.defaultOptions"

	if t.call == "" {
		t.call = "./assets/RHVoice -p artemiy -R 16000 -o /dev/stdout"
	}

	t.mode = ttsModeScript

	if t.readTimeout == 0 {
		t.readTimeout = 2
	}

	t.log.Info("Set default options",
		zap.String("op", op))
}

// TTS makes audio from text
// Use WebSockets for streaming
// Returned bytes are audio data
func (t *TTS) TTS(w http.ResponseWriter, r *http.Request) {
	const op = "tts.TTS"

	defer t.log.Debug("Leave", zap.String("op", op))

	if t.call == "" {
		t.defaultOptions()
	}

	t.log.Info("TTS request",
		zap.String("op", op),
		zap.String("call", t.call),
		zap.String("voiceID", t.voiceID),
		zap.String("modelName", t.modelName),
		zap.String("readTimeout", fmt.Sprintf("%d", t.readTimeout)))

	conn, err := t.upg.Upgrade(w, r, nil)
	if err != nil {
		t.log.Error("Failed to upgrade connection",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to upgrade connection",
			http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	t.log.Info("Upgraded connection")

	comBuf := rb.NewRB[byte](defaultLength)

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				t.log.Info("Context done")
				return
			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					t.log.Error("Failed to read message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				comBuf.Write(msg)
			}
		}
	}()

	textBufPtr := bufPool.Get().(*[]byte)
	bytesPCMptr := bufPool.Get().(*[]byte)
	floatBufPtr := audioBufPool.Get().(*[]float32)

	textBuf := (*textBufPtr)[:defaultLength]
	bytesPCM := (*bytesPCMptr)[:defaultLength]

	defer func() {
		bufPool.Put(textBufPtr)
		bufPool.Put(bytesPCMptr)
		audioBufPool.Put(floatBufPtr)
	}()

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				t.log.Info("Context done")
				return
			default:
				if comBuf.Len() == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				cut := comBuf.Read(textBuf[:cap(textBuf)])
				textBuf = textBuf[:cut]

				t.log.Info("Got text",
					zap.String("op", op),
					zap.String("text", unsafe.String(unsafe.SliceData(textBuf), len(textBuf))))

				switch t.mode {
				case ttsModeScript:
					bytesPCM, err = t.callScript(t.call, textBuf)
					if err != nil {
						t.log.Error("Failed to call script",
							zap.String("op", op),
							zap.Error(err))
						return

					}
				case ttsModeAPI:
					bytesPCM, err = t.callAPI(t.call, textBuf)
					if err != nil {
						t.log.Error("Failed to call API",
							zap.String("op", op),
							zap.Error(err))
						return
					}
				}

				if len(bytesPCM) == 0 {
					t.log.Warn("Empty PCM data",
						zap.String("op", op))
					return
				}

				t.log.Info("Got data",
					zap.String("op", op),
					zap.Int("res length", len(bytesPCM)))

				if err := conn.WriteMessage(websocket.BinaryMessage, bytesPCM); err != nil {
					t.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}
			}
		}
	}()

	<-ctx.Done()
	t.log.Info("Context done", zap.String("op", op))
}

// callScript calls script for making audio from text
func (t *TTS) callScript(scriptCall string, textBuf []byte) ([]byte, error) {
	return callScript(scriptCall, textBuf, t.log)
}

// callAPI calls API for making audio from text
func (t *TTS) callAPI(url string, textBuf []byte) ([]byte, error) {
	const op = "tts.callAPI"

	t.log.Info("Call API",
		zap.String("op", op),
		zap.String("url", url))

	req := struct {
		Text      string `json:"text"`
		VoiceID   string `json:"voice_id"`
		ModelName string `json:"model_name"`
	}{
		Text:      unsafe.String(unsafe.SliceData(textBuf), len(textBuf)),
		VoiceID:   t.voiceID,
		ModelName: t.modelName,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to marshal request body: %w", op, err)
	}

	resp, err := t.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("%s: failed to send request: %w", op, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: request failed, status: %d", op, resp.StatusCode)
	}

	var res struct {
		WAV string `json:"wav"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("%s: failed to decode response body: %w", op, err)
	}

	t.log.Info("Received response",
		zap.String("op", op),
		zap.Int("res length", len(res.WAV)))

	resBytes := unsafe.Slice(unsafe.StringData(res.WAV), len(res.WAV))

	return resBytes, nil
}
