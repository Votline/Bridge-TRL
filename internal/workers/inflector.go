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
	"net/http"
	"strconv"
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
}

// NewInflector creates a new Inflector worker
func NewInflector(log *zap.Logger) *Inflector {
	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	return &Inflector{
		Name: "Inflector",
		log:  log,
		upg:  upg,
	}
}

// GetName returns the name of the worker
// Used for logging
func (i *Inflector) GetName() string {
	return i.Name
}

// Register the worker endpoints on the http.ServeMux
func (i *Inflector) Register(m *http.ServeMux) {
	m.HandleFunc("/inflector", i.Inflector)
	m.HandleFunc("/inflector/setOptions", i.setOptions)
}

// setOptions sets options of the worker
// Used for setting call, model and readTimeout
// Call is URL or path to script, which inflects text
// Model is name of the AI model
// ReadTimeout is a timeout during which messages are collected
func (i *Inflector) setOptions(w http.ResponseWriter, r *http.Request) {
	const op = "inflector.setCall"

	var req struct {
		Call        string `json:"call"`
		Model       string `json:"model"`
		ReadTimeout int    `json:"read_timeout"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		i.log.Error("Failed to decode request",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to decode request",
			http.StatusInternalServerError)
		return
	}

	i.model = req.Model

	if req.ReadTimeout == 0 {
		req.ReadTimeout = 2
	}

	i.mode = inflectorModeAPI
	if has := findAPIPrefix(req.Call); !has {
		i.mode = inflectorModeScript
	}

	i.readTimeout = time.Duration(req.ReadTimeout) * time.Second

	client := &http.Client{
		Timeout: i.readTimeout,
	}
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	i.client = client
	i.call = req.Call

	i.log.Info("Set options",
		zap.String("op", op),
		zap.String("call", req.Call),
		zap.String("model", req.Model),
		zap.String("readTimeout", fmt.Sprintf("%d", req.ReadTimeout)))

	w.WriteHeader(http.StatusOK)
}

func (i *Inflector) initTranslator() {
	const op = "inflector.initTranslator"

	i.call = ""
}

// Inflector makes audio from text
// Use WebSockets for streaming
// Returned bytes are audio data
func (i *Inflector) Inflector(w http.ResponseWriter, r *http.Request) {
	const op = "inflector.Inflector"

	defer i.log.Debug("Leave", zap.String("op", op))

	if i.call == "" {
		i.call = "http://localhost:11434/api/generate"
		i.model = "gemma2:2b_Q4_K_M"
		i.readTimeout = 60 * time.Second
		i.mode = inflectorModeAPI
		i.client = nil
	}

	i.log.Info("Inflector request",
		zap.String("op", op),
		zap.String("call", i.call),
		zap.String("model", i.model),
		zap.String("readTimeout", fmt.Sprintf("%d", i.readTimeout)),
		zap.String("mode", fmt.Sprintf("%d", i.mode)))

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

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

	i.log.Info("Upgraded connection", zap.String("op", op))

	origBuf := rb.NewRB[byte](defaultLength)
	tranBuf := rb.NewRB[byte](defaultLength)

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

				data, err := unmarshalWSMessage(msg)
				if err != nil {
					i.log.Error("Failed to unmarshal message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				origBytes := unsafe.Slice(unsafe.StringData(data.Original), len(data.Original))
				tranBytes := unsafe.Slice(unsafe.StringData(data.Translated), len(data.Translated))

				origBuf.Write(origBytes)
				tranBuf.Write(tranBytes)

				i.log.Info("Message received",
					zap.String("op", op),
					zap.String("original", data.Original),
					zap.String("translated", data.Translated))
			}
		}
	}()

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

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				i.log.Info("Context done", zap.String("op", op))
				return
			default:
				if tranBuf.Len() == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				nTran := tranBuf.Read(buf)
				tranFinal = append(tranFinal, buf[:nTran]...)

				nOrig := origBuf.Read(buf)
				origFinal = append(origFinal, buf[:nOrig]...)

				inflected, err := i.sendInflect(origFinal, tranFinal)
				if err != nil {
					i.log.Error("Failed to send request",
						zap.String("op", op),
						zap.Error(err),
						zap.String("orig", unsafe.String(unsafe.SliceData(origFinal), len(origFinal))),
						zap.String("tran", unsafe.String(unsafe.SliceData(tranFinal), len(tranFinal))))
				}

				i.log.Info("Send inflected text",
					zap.String("op", op),
					zap.String("inflected", unsafe.String(unsafe.SliceData(inflected), len(inflected))))

				err = conn.WriteMessage(websocket.TextMessage, inflected)
				if err != nil {
					i.log.Error("Failed to write message", zap.Error(err))
					return
				}

				origFinal = origFinal[:0]
				tranFinal = tranFinal[:0]
			}
		}
	}()

	<-ctx.Done()
	i.log.Info("Context done", zap.String("op", op))
}

// sendInflect sends inflected text to the server
// return inflicted text and error
func (i *Inflector) sendInflect(orig, tran []byte) ([]byte, error) {
	const op = "inflector.sendInflect"
	if len(tran) == 0 {
		return nil, nil
	}

	call := i.call

	if i.client == nil {
		client := &http.Client{
			Timeout: i.readTimeout,
		}
		i.client = client
	}

	step := 128
	resultPtr := bufPool.Get().(*[]byte)
	result := (*resultPtr)[:0]
	defer bufPool.Put(resultPtr)

	for len(orig) > 0 && len(tran) > 0 {
		origStartIdx := 0
		if step > len(orig) {
			origStartIdx = len(orig)
		} else {
			origStartIdx = bytes.IndexByte(orig[step:], ' ')
			if origStartIdx == -1 {
				origStartIdx = len(orig)
			} else {
				origStartIdx += step
			}
		}
		origSpliced := orig[:origStartIdx]
		orig = orig[origStartIdx:]

		tranStartIdx := 0
		if step > len(tran) {
			tranStartIdx = len(tran)
		} else {
			tranStartIdx = bytes.IndexByte(tran[step:], ' ')
			if tranStartIdx == -1 {
				tranStartIdx = len(tran)
			} else {
				tranStartIdx += step
			}
		}
		tranSpliced := tran[:tranStartIdx]
		tran = tran[tranStartIdx:]

		origStr := unsafe.String(unsafe.SliceData(origSpliced), len(origSpliced))
		tranStr := unsafe.String(unsafe.SliceData(tranSpliced), len(tranSpliced))
		switch i.mode {
		case inflectorModeScript:
			payload := map[string]string{
				"original":   origStr,
				"translated": tranStr,
			}

			jsonData, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("%s: failed to marshal request body: %w", op, err)
			}

			resBytes, err := i.callScript(call, jsonData, i.log)
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

			result = append(result, resBytes...)
			result = append(result, ' ')

		case inflectorModeAPI:
			body := RequestBody{
				Model:  i.model,
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

			resBytes, err := i.callAPI(call, jsonData, origStr, tranStr)
			if err != nil {
				return nil, fmt.Errorf("%s: failed to send request: %w", op, err)
			}

			result = append(result, resBytes...)
			result = append(result, ' ')

		default:
			i.log.Error("Unknown mode", zap.Int("mode", i.mode))
			return nil, fmt.Errorf("%s: unknown mode: %d", op, i.mode)
		}
	}

	return result, nil
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
func (i *Inflector) callAPI(url string, jsonData []byte, origStr, tranStr string) ([]byte, error) {
	const op = "inflector.callAPI"

	i.log.Info("Send request",
		zap.String("op", op),
		zap.String("url", url),
		zap.String("body", fmt.Sprintf("Original: %s\nTranslated: %s", origStr, tranStr)))

	resp, err := i.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
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

	i.log.Info("Received response",
		zap.String("op", op),
		zap.String("response", res.Response))

	resBytes := unsafe.Slice(unsafe.StringData(res.Response), len(res.Response))
	trimSpaceBytes(&resBytes)

	return resBytes, nil
}

// callScript calls script for inflecting text
func (i *Inflector) callScript(scriptCall string, jsonData []byte, log *zap.Logger) ([]byte, error) {
	const op = "inflector.callScript"

	res, err := callScript(scriptCall, jsonData, log)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to call script: %w", op, err)
	}

	trimSpaceBytes(&res)

	return res, nil
}

// findJSONKey finds key in raw json bytes
// Support escaped values and quotes
// Returned unquoted value
func findJSONKey(msg []byte, key []byte) (string, error) {
	const op = "workers.findJSONKey"

	idx := bytes.Index(msg, key)
	if idx == -1 {
		return "", fmt.Errorf("%s: failed to find %q key", op, key)
	}

	keyEnd := idx + len(key)

	start := bytes.IndexByte(msg[keyEnd:], '"')
	if start == -1 {
		return "", fmt.Errorf("%s: failed to find '", op)
	}
	start += keyEnd

	curStart := start + 1 // skip '"'
	end := 0
	for {
		estEnd := bytes.IndexByte(msg[curStart:], '"')
		if estEnd == -1 {
			return "", fmt.Errorf("%s: failed to find '", op)
		}
		estEnd += curStart
		if msg[estEnd-1] == '\\' && msg[estEnd-2] != '\\' { // user escaped '"', not json
			curStart = estEnd + 1
			continue
		} else {
			end = estEnd
			break
		}
	}

	end++ // add '"' for strconv.Unquote
	rawBytes := msg[start:end]
	rawStr := unsafe.String(unsafe.SliceData(rawBytes), len(rawBytes))

	unquotedStr, err := strconv.Unquote(rawStr)
	if err != nil {
		return "", fmt.Errorf("%s: failed to unquote original message: %w", op, err)
	}

	return unquotedStr, nil
}

// unmarshalWSMessage unmarshals raw websocket message
// Used 'original' and 'translated' keys
// Returned WSMessage struct with original and translated texts
func unmarshalWSMessage(msg []byte) (WSMessage, error) {
	const op = "workers.unmarshalWSMessage"
	var data WSMessage

	original, err := findJSONKey(msg, []byte(`"original":`))
	if err != nil {
		return data, fmt.Errorf("%s: failed to find original message: %w", op, err)
	}

	translated, err := findJSONKey(msg, []byte(`"translated":`))
	if err != nil {
		return data, fmt.Errorf("%s: failed to find translated message: %w", op, err)
	}

	data.Original = original
	data.Translated = translated

	return data, nil
}
