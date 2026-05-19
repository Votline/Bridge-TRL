// Package workers inflector.go contains worker implementations for text inflection
// Adjust word forms grammatical agreement
// Returned text is in the corrected grammatical form
package workers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
	"unsafe"

	rb "btrl/internal/ringbuffer"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Options is a struct for AI options
type Options struct {
	Temperature   float64 `json:"temperature"`
	NumPredict    int     `json:"num_predict"`
	TopP          float64 `json:"top_p"`
	RepeatPenalty float64 `json:"repeat_penalty"`
}

// RequestBody is a struct for HTTP request body
type RequestBody struct {
	Model   string  `json:"model"`
	System  string  `json:"system"`
	Prompt  string  `json:"prompt"`
	Stream  bool    `json:"stream"`
	Options Options `json:"options"`
}

// WSMessage is a struct for applied WebSocket message
type WSMessage struct {
	Original   string `json:"original"`
	Translated string `json:"translated"`
}

// Inflector modes
const (
	// InflectorModeScript is used for calling script, which inflects text
	inflectorModeScript = -2

	// InflectorModeAPI is used for calling API, which inflects text
	inflectorModeAPI = -3

	// prompt is a prompt for AI
	prompt = `You are a strict linguistic engine.
Task: Fix grammar, morphology, and syntax in the provided text.
Constraints:
1. Do NOT change the meaning.
2. Do NOT use synonyms if the original word is grammatically correct.
3. Output ONLY the corrected text.
4. No explanations.
5. The output MUST be in the same language as the TRANSLATION.`
)

// Inflector struct for implementing worker
// Contains 'tts' endpoint for make audio from text
type Inflector struct {
	// Name of the worker
	Name string

	// Call is URL or path to script, which inflects text
	// You can use offline ollama AI or any API
	call string

	// Model is name of the AI model
	// Used for requests to offline ollama AI
	model string

	// Mode is a mode of the worker
	// Can be: script or api
	// Script mode is used for calling script, which inflects text
	// API mode is used for calling API, which inflects text
	mode int

	// ReadTimeout is a timeout during which messages are collected
	// This is necessary in order not to send short messages, but whole sentences.
	// ReadTimeout only works if no point is found.
	// Default is 2 seconds
	readTimeout time.Duration

	// client is a client for send requests
	client *http.Client
	log    *zap.Logger
	upg    websocket.Upgrader
	ctx    context.Context
	cancel context.CancelFunc
}

// NewInflector creates a new Inflector worker
func NewInflector(log *zap.Logger) *Inflector {
	ctx, cancel := context.WithCancel(context.Background())
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &Inflector{
		Name:   "Inflector",
		log:    log,
		upg:    upg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// GetName returns the name of the worker
// Used for logging
func (t *Inflector) GetName() string {
	return t.Name
}

// Register the worker endpoints on the http.ServeMux
func (t *Inflector) Register(m *http.ServeMux) {
	m.HandleFunc("/inflector", t.Inflector)
	m.HandleFunc("/inflector/setOptions", t.setOptions)
}

// Close cancels the context
// Cancel context is used for shutdown WS connections
func (t *Inflector) Close(ctx context.Context) {
	t.cancel()
}

// setOptions sets options of the worker
// Used for setting call, model and readTimeout
// Call is URL or path to script, which inflects text
// Model is name of the AI model
// ReadTimeout is a timeout during which messages are collected
func (t *Inflector) setOptions(w http.ResponseWriter, r *http.Request) {
	const op = "inflector.setCall"

	var req struct {
		Call        string `json:"call"`
		Model       string `json:"model"`
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

	t.model = req.Model

	if req.ReadTimeout == 0 {
		req.ReadTimeout = 2
	}

	t.mode = inflectorModeAPI
	if has := findAPIPrefix(req.Call); !has {
		t.mode = inflectorModeScript
	}

	t.readTimeout = time.Duration(req.ReadTimeout) * time.Second

	client := &http.Client{
		Timeout: t.readTimeout,
	}
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	t.client = client
	t.call = req.Call

	t.log.Info("Set options",
		zap.String("op", op),
		zap.String("call", req.Call),
		zap.String("model", req.Model),
		zap.String("readTimeout", fmt.Sprintf("%d", req.ReadTimeout)))

	w.WriteHeader(http.StatusOK)
}

func (t *Inflector) initTranslator() {
	const op = "inflector.initTranslator"

	t.call = ""
}

// Inflector makes audio from text
// Use WebSockets for streaming
// Returned bytes are audio data
func (t *Inflector) Inflector(w http.ResponseWriter, r *http.Request) {
	const op = "inflector.Inflector"

	if t.call == "" {
		t.call = "http://localhost:11434/api/generate"
		t.model = "gemma2:2b_Q4_K_M"
		t.readTimeout = 60 * time.Second
		t.mode = inflectorModeAPI
		t.client = nil
	}

	t.log.Info("Inflector request",
		zap.String("op", op),
		zap.String("call", t.call),
		zap.String("model", t.model),
		zap.String("readTimeout", fmt.Sprintf("%d", t.readTimeout)),
		zap.String("mode", fmt.Sprintf("%d", t.mode)))

	t.ctx = r.Context()

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

	t.log.Info("Upgraded connection", zap.String("op", op))

	origBuf := rb.NewRB[byte](defaultLength)
	tranBuf := rb.NewRB[byte](defaultLength)

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-t.ctx.Done():
				t.log.Info("Context done", zap.String("op", op))
				return
			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					t.log.Error("Failed to read message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				var data WSMessage
				if err := json.Unmarshal(msg, &data); err != nil {
					t.log.Error("Failed to unmarshal message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				// TODO: custom json parser instead of json

				origBytes := unsafe.Slice(unsafe.StringData(data.Original), len(data.Original))
				tranBytes := unsafe.Slice(unsafe.StringData(data.Translated), len(data.Translated))

				origBuf.Write(origBytes)
				tranBuf.Write(tranBytes)

				t.log.Info("Message received",
					zap.String("op", op),
					zap.String("original", data.Original),
					zap.String("translated", data.Translated))
			}
		}
	})

	origFinalPtr := bufPool.Get().(*[]byte)
	tranFinalPtr := bufPool.Get().(*[]byte)
	bufPtr := bufPool.Get().(*[]byte)

	defer func() {
		bufPool.Put(origFinalPtr)
		bufPool.Put(tranFinalPtr)
		bufPool.Put(bufPtr)
	}()

	origFinal := (*origFinalPtr)[:0]
	tranFinal := (*tranFinalPtr)[:0]
	buf := (*bufPtr)

	wg.Go(func() {
		for {
			select {
			case <-t.ctx.Done():
				t.log.Info("Context done", zap.String("op", op))
				return
			default:
				if tranBuf.Len() == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				nTran := tranBuf.Read(buf)
				tranFinal = append(tranFinal, buf[:nTran]...)

				if !bytes.ContainsAny(buf[:nTran], ".?!") {
					continue
				}

				nOrig := origBuf.Read(buf)
				origFinal = append(origFinal, buf[:nOrig]...)

				inflected, err := t.sendInflect(origFinal, tranFinal)
				if err != nil {
					t.log.Error("Failed to send request",
						zap.String("op", op),
						zap.Error(err),
						zap.String("orig", unsafe.String(unsafe.SliceData(origFinal), len(origFinal))),
						zap.String("tran", unsafe.String(unsafe.SliceData(tranFinal), len(tranFinal))))
				}

				t.log.Info("Send inflected text",
					zap.String("op", op),
					zap.String("inflected", unsafe.String(unsafe.SliceData(inflected), len(inflected))))

				err = conn.WriteMessage(websocket.TextMessage, inflected)
				if err != nil {
					t.log.Error("Failed to write message", zap.Error(err))
					return
				}

				origFinal = origFinal[:0]
				tranFinal = tranFinal[:0]
			}
		}
	})

	wg.Wait()
}

// sendInflect sends inflected text to the server
// return inflicted text and error
func (t *Inflector) sendInflect(orig, tran []byte) ([]byte, error) {
	const op = "inflector.sendInflect"
	if len(tran) == 0 {
		return nil, nil
	}

	origStr := unsafe.String(unsafe.SliceData(orig), len(orig))
	tranStr := unsafe.String(unsafe.SliceData(tran), len(tran))

	call := t.call

	if t.client == nil {
		client := &http.Client{
			Timeout: t.readTimeout,
		}
		t.client = client
	}

	switch t.mode {
	case inflectorModeScript:
		payload := map[string]string{
			"original":   origStr,
			"translated": tranStr,
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to marshal request body: %w", op, err)
		}

		resBytes, err := t.callScript(call, jsonData)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to call script: %w", op, err)
		}

		var res struct {
			Response string `json:"response"`
		}

		if err := json.Unmarshal(resBytes, &res); err != nil {
			return nil, fmt.Errorf("%s: failed to unmarshal response body: %w", op, err)
		}

		resBytes = unsafe.Slice(unsafe.StringData(res.Response), len(res.Response))
		trimSpaceBytes(&resBytes)

		return resBytes, nil

	case inflectorModeAPI:
		body := RequestBody{
			Model:  t.model,
			System: prompt,
			Prompt: fmt.Sprintf("Original: %s\nTranslated: %s", origStr, tranStr),
			Stream: false,
			Options: Options{
				Temperature:   0.1,
				NumPredict:    512,
				TopP:          0.9,
				RepeatPenalty: 1.1,
			},
		}

		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to marshal request body: %w", op, err)
		}

		resBytes, err := t.callAPI(call, jsonData, origStr, tranStr)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to send request: %w", op, err)
		}
		return resBytes, nil

	default:
		t.log.Error("Unknown mode", zap.Int("mode", t.mode))
		return nil, fmt.Errorf("%s: unknown mode: %d", op, t.mode)
	}
}

// trimSpaceBytes trims spaces from data by pointer
func trimSpaceBytes(b *[]byte) {
	tempB := *b

	start := 0
	end := len(tempB) - 1

	for start < end && isSpace(tempB[start]) {
		start++
	}
	for end > start && isSpace(tempB[end]) {
		end--
	}

	*b = tempB[start : end+1]
}

// isSpace returns true if byte is space
func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// sendHTTP sends text for inflecting to the server
// return inflicted text and error
func (t *Inflector) callAPI(url string, jsonData []byte, origStr, tranStr string) ([]byte, error) {
	const op = "inflector.callAPI"

	t.log.Info("Send request",
		zap.String("op", op),
		zap.String("url", url),
		zap.String("body", fmt.Sprintf("Original: %s\nTranslated: %s", origStr, tranStr)))

	resp, err := t.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("%s: failed to send request: %w", op, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: request failed, status: %d", op, resp.StatusCode)
	}

	var res struct {
		Response string `json:"response"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("%s: failed to decode response body: %w", op, err)
	}

	t.log.Info("Received response",
		zap.String("op", op),
		zap.String("response", res.Response))

	resBytes := unsafe.Slice(unsafe.StringData(res.Response), len(res.Response))
	trimSpaceBytes(&resBytes)

	return resBytes, nil
}

func (t *Inflector) callScript(scriptCall string, jsonData []byte) ([]byte, error) {
	const op = "inflector.callScript"

	t.log.Info("Call script",
		zap.String("op", op),
		zap.String("script call", scriptCall))

	parts := strings.Split(scriptCall, " ")

	cmd := exec.Command(parts[0], parts[1:]...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("%s: failed to get stdin pipe: %w", op, err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("%s: failed to get stdout pipe: %w", op, err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%s: failed to start script: %w", op, err)
	}

	if _, err := stdin.Write(jsonData); err != nil {
		return nil, fmt.Errorf("%s: failed to write to stdin: %w", op, err)
	}

	if err := stdin.Close(); err != nil {
		return nil, fmt.Errorf("%s: failed to close stdin: %w", op, err)
	}

	resBytes, err := io.ReadAll(stdout)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to read stdout: %w", op, err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("%s: failed to wait for script: %w", op, err)
	}

	trimSpaceBytes(&resBytes)

	return resBytes, nil
}
