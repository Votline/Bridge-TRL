// Package workers inflector.go contains worker implementations for text inflection
// Adjust word forms grammatical agreement
// Returned text is in the corrected grammatical form
package workers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
	prompt = `
	You are a universal text polisher.
Task: Correct the 'Translated' text using the 'Original' text as a semantic and structural reference.

Instructions:
1. Fix all morphological and syntactical errors in 'Translated'.
2. Align gender, number, and case agreements based on the context of 'Original'.
3. The output MUST be in the same language as the 'Translated' input.
4. Keep the style, but ensure the flow is natural for a native speaker.
5. Output ONLY the corrected text without any meta-talk or labels.

Original: {orig}
Translated: {tran}
	`
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
					if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
						i.log.Warn("Connection closed", zap.String("op", op))
						return
					}

					var netErr net.Error
					if errors.As(err, &netErr) && netErr.Timeout() {
						continue
					}

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

	resultBuf := ringBufPool.Get().(*rb.RingBuffer[byte])
	resultBuf.Reset()
	defer ringBufPool.Put(resultBuf)

	inflectDone := make(chan struct{}, 1)
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

				if err := i.sendInflect(origFinal, tranFinal, resultBuf); err != nil {
					i.log.Error("Failed to send request",
						zap.String("op", op),
						zap.Error(err),
						zap.String("orig", unsafe.String(unsafe.SliceData(origFinal), len(origFinal))),
						zap.String("tran", unsafe.String(unsafe.SliceData(tranFinal), len(tranFinal))))
				}
				origFinal = origFinal[:0]
				tranFinal = tranFinal[:0]

				i.log.Debug("Inflect done",
					zap.String("op", op))

				inflectDone <- struct{}{}
			}
		}
	}()

	sendBufPtr := bufPool.Get().(*[]byte)
	sendBuf := (*sendBufPtr)[:defaultLength]
	defer bufPool.Put(sendBufPtr)
	sendLen := defaultLength / 2
	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				i.log.Info("Context done", zap.String("op", op))
				return
			case <-inflectDone:
				for resultBuf.Len() > 0 {
					n := resultBuf.Read(sendBuf)
					inflected := sendBuf[:n]
					i.log.Info("Send inflected text after done",
						zap.String("op", op),
						zap.String("inflected", unsafe.String(unsafe.SliceData(inflected), len(inflected))))

					if err = conn.WriteMessage(websocket.TextMessage, inflected); err != nil {
						i.log.Error("Failed to write message after done",
							zap.String("op", op),
							zap.Error(err))
						return
					}
				}
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"done": true}`)); err != nil {
					i.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}
			default:
				if resultBuf.Len() < sendLen && !resultBuf.IsClosed() {
					time.Sleep(10 * time.Millisecond)
					continue
				} else if resultBuf.IsClosed() {
					resultBuf.Open()
				}

				n := resultBuf.Read(sendBuf)
				if n == 0 {
					i.log.Warn("Result buffer is empty, leaving",
						zap.String("op", op))
					break
				}

				inflected := sendBuf[:n]
				i.log.Info("Send inflected text",
					zap.String("op", op),
					zap.String("inflected", unsafe.String(unsafe.SliceData(inflected), len(inflected))))

				if err = conn.WriteMessage(websocket.TextMessage, inflected); err != nil {
					i.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}
			}
		}
	}()

	<-ctx.Done()
	i.log.Info("Context done", zap.String("op", op))
}

// sendInflect sends inflected text to the server
// return inflicted text and error
func (i *Inflector) sendInflect(orig, tran []byte, result *rb.RingBuffer[byte]) error {
	const op = "inflector.sendInflect"
	if len(tran) == 0 {
		return nil
	}

	call := i.call
	result.Open()

	if i.client == nil {
		client := &http.Client{
			Timeout: i.readTimeout,
		}
		i.client = client
	}

	origStr := unsafe.String(unsafe.SliceData(orig), len(orig))
	tranStr := unsafe.String(unsafe.SliceData(tran), len(tran))

	switch i.mode {
	case inflectorModeScript:
		payload := map[string]string{
			"original":   origStr,
			"translated": tranStr,
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("%s: failed to marshal request body: %w", op, err)
		}

		resBytes, err := i.callScript(call, jsonData, i.log)
		if err != nil {
			return fmt.Errorf("%s: failed to call script: %w", op, err)
		}

		var res struct {
			Response string `json:"response"`
		}

		if err := json.Unmarshal(resBytes, &res); err != nil {
			return fmt.Errorf("%s: failed to unmarshal response body: %w", op, err)
		}

		resBytes = unsafe.Slice(unsafe.StringData(res.Response), len(res.Response))
		trimSpaceBytes(&resBytes)

		result.Write(resBytes)
		result.Write([]byte{' '})

	case inflectorModeAPI:
		body := RequestBody{
			Model:  i.model,
			System: prompt,
			Prompt: fmt.Sprintf("Original: %s\nTranslated: %s", origStr, tranStr),
			Stream: true,
			Options: Options{
				Temperature:   0.1,
				NumPredict:    512,
				TopP:          0.9,
				RepeatPenalty: 1.1,
			},
		}

		jsonData, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%s: failed to marshal request body: %w", op, err)
		}

		if err := i.callAPI(call, jsonData, origStr, tranStr, result); err != nil {
			return fmt.Errorf("%s: failed to send request: %w", op, err)
		}

	default:
		i.log.Error("Unknown mode", zap.Int("mode", i.mode))
		return fmt.Errorf("%s: unknown mode: %d", op, i.mode)
	}

	return nil
}

// sendHTTP sends text for inflecting to the server
// return inflicted text and error
func (i *Inflector) callAPI(url string, jsonData []byte, origStr, tranStr string, result *rb.RingBuffer[byte]) error {
	const op = "inflector.callAPI"

	i.log.Info("Send request",
		zap.String("op", op),
		zap.String("url", url),
		zap.String("body", fmt.Sprintf("Original: %s\nTranslated: %s", origStr, tranStr)))

	resp, err := i.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("%s: failed to send request: %w", op, err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var streamRes struct {
			Response string `json:"response"`
			Message  struct {
				Content string `json:"content"`
			} `json:"message"`
			Done bool `json:"done"`
		}

		if err := json.Unmarshal(scanner.Bytes(), &streamRes); err != nil {
			return fmt.Errorf("%s: failed to unmarshal response body: %w", op, err)
		}

		text := streamRes.Response
		if text == "" {
			text = streamRes.Message.Content
		}

		if text != "" {
			textBytes := unsafe.Slice(unsafe.StringData(text), len(text))
			result.Write(textBytes)
		}

		if streamRes.Done {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("%s: failed to read response body: %w", op, err)
	}

	result.Close()

	return nil
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
