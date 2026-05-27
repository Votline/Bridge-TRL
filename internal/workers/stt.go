// Package workers stt.go contains worker implementations for speech to text
// Speech to text
// Returned text is in the source language
package workers

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
	"unsafe"

	rb "btrl/internal/ringbuffer"

	gd "github.com/Votline/Go-audio"
	"github.com/alphacep/vosk-api/go"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	sampleRateSTT = 16000.0
	trashLen      = len(`"partial" : "`)
)

// STT struct for implementing worker
// Contains 'stt' endpoint for speech to text
type STT struct {
	// Name of the worker
	Name string

	// vosk is a recognizer for speech to text
	vosk *vosk.VoskRecognizer

	log *zap.Logger
	upg websocket.Upgrader
}

// NewSTT creates a new STT worker
func NewSTT(log *zap.Logger) (*STT, error) {
	const op = "workers.NewSTT"

	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	return &STT{
		Name: "STT",
		vosk: nil,
		log:  log,
		upg:  upg,
	}, nil
}

// GetName returns the name of the worker
// Used for logging
func (s *STT) GetName() string {
	return s.Name
}

// Register the worker endpoints on the http.ServeMux
func (s *STT) Register(m *http.ServeMux) {
	m.HandleFunc("/stt", s.STT)
	m.HandleFunc("/stt/setOptions", s.setOptions)
}

// Close frees the model
func (s *STT) Close(ctx context.Context) {
	if s.vosk != nil {
		s.vosk.Free()
	}
}

// setOptions create model and recognizer
// ModelPath is path to model
func (s *STT) setOptions(w http.ResponseWriter, r *http.Request) {
	const op = "workers.STT.setOptions"

	var req struct {
		ModelPath string `json:"model_Path"`
	}

	s.log.Info("Set options",
		zap.String("op", op))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.log.Error("Failed to decode request",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to decode request",
			http.StatusInternalServerError)
		return
	}

	if err := s.initModel(req.ModelPath); err != nil {
		s.log.Error("Failed to init model",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to init model",
			http.StatusInternalServerError)
		return
	}

	s.log.Info("Set model path",
		zap.String("op", op),
		zap.String("modelPath", req.ModelPath))

	w.WriteHeader(http.StatusOK)
}

// initModel creates recognizer for speech to text
func (s *STT) initModel(modelPath string) error {
	const op = "workers.STT.initModel"

	if modelPath == "" {
		modelPath = "assets/vosk-model-small-ru-0.22"
	}

	model, err := vosk.NewModel(modelPath)
	if err != nil {
		return fmt.Errorf("%s: new vosk model: %w", op, err)
	}

	rec, err := vosk.NewRecognizer(model, sampleRateSTT)
	if err != nil {
		return fmt.Errorf("%s: new vosk recognizer: %w", op, err)
	}

	rec.SetMaxAlternatives(0)

	s.vosk = rec

	return nil
}

// STT speech to text
// Use WebSockets for streaming
// Returned text is in the source language
func (s *STT) STT(w http.ResponseWriter, r *http.Request) {
	const op = "workers.STT"

	defer s.log.Debug("Leave", zap.String("op", op))

	if s.vosk == nil {
		if err := s.initModel(""); err != nil {
			s.log.Error("Failed to init model",
				zap.String("op", op),
				zap.Error(err))
			http.Error(w, "Failed to init model",
				http.StatusInternalServerError)
			return
		}
	}

	s.log.Info("STT request",
		zap.String("op", op))

	s.vosk.Reset()

	conn, err := s.upg.Upgrade(w, r, nil)
	if err != nil {
		s.log.Error("Failed to upgrade connection",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "Failed to upgrade connection",
			http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	s.log.Info("Upgraded connection",
		zap.String("op", op))

	// audioBuf used for receiving audio data from WS message
	audioBuf := gd.NewRingBuffer(defAudioLen)

	// gotPCM used for converting audio data to float32 slice
	gotPCMPtr := audioBufPool.Get().(*[]float32)
	gotPCM := (*gotPCMPtr)[:0]
	defer audioBufPool.Put(gotPCMPtr)

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				s.log.Info("Context done", zap.String("op", op))
				return
			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					s.log.Error("Failed to read message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				if len(msg) == 0 {
					continue
				}

				s.log.Info("Got PCM",
					zap.String("op", op),
					zap.Int("len", len(msg)))

				bytesToFloat32(msg, &gotPCM)
				audioBuf.Write(gotPCM)
			}
		}
	}()

	// resBuf used for containing all parsed text from Vosk response
	resBuf := ringBufPool.Get().(*rb.RingBuffer[byte])
	defer ringBufPool.Put(resBuf)

	// res used for read parts from resBuf and send to WS
	resPtr := bufPool.Get().(*[]byte)
	res := (*resPtr)[:defaultLength]
	defer bufPool.Put(resPtr)

	// sendPCM used for read audio from audioBuf and send to processAudio
	sendPCMPtr := audioBufPool.Get().(*[]float32)
	sendPCM := (*sendPCMPtr)[:defAudioLen]
	defer audioBufPool.Put(sendPCMPtr)

	// int16Smp used for converting audio data to int16 slice
	int16SmpPtr := int16AudioBufPool.Get().(*[]int16)
	int16Smp := (*int16SmpPtr)[:defAudioLen]
	defer int16AudioBufPool.Put(int16SmpPtr)

	start := 0
	estEnd := 0
	skipCnt := 0
	end := (defaultLength / 4) - trashLen
	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				s.log.Info("Context done", zap.String("op", op))
				return
			default:
				if audioBuf.Len() == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				if len(sendPCM) < audioBuf.Len() {
					sendPCM = make([]float32, audioBuf.Len())
				}

				cut := audioBuf.Read(sendPCM)
				pcm := sendPCM[:cut]

				s.processAudio(pcm, resBuf, int16Smp, &start, &estEnd, &end, &skipCnt)
			}
		}
	}()

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				s.log.Info("Context done", zap.String("op", op))
				return
			default:
				cut := resBuf.Read(res)
				temp := res[:cut]

				if err := conn.WriteMessage(websocket.TextMessage, temp); err != nil {
					s.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				s.log.Info("Sent message",
					zap.String("op", op),
					zap.Int("len", len(res)))
			}
		}
	}()

	<-ctx.Done()
	s.log.Info("Context done", zap.String("op", op))
}

// processAudio send audio data to Vosk and send text to buffer
// It collect partial result from Vosk and send sliding window to buffer
// pcm - audio data, resBuf - buffer for result
// cursors: start - start of sliding window, estEnd - estimated end of sliding window,
// end - end of sliding window, skipCnt - counter for skipping audio data
// skipCnt used for waiting vosk to finish processing last word in current window
func (s *STT) processAudio(pcm []float32, resBuf *rb.RingBuffer[byte], int16Samples []int16, start, estEnd, end, skipCnt *int) {
	const op = "workers.STT.processAudio"

	if len(pcm) == 0 {
		s.log.Warn("Empty pcm float32 data",
			zap.String("op", op))
		return
	}

	float32ToVosk(pcm, int16Samples)
	if len(int16Samples) == 0 {
		s.log.Warn("Empty pcm int16 data",
			zap.String("op", op))
		return
	}
	bytesSamples := unsafe.Slice((*byte)(unsafe.Pointer(&int16Samples[0])), len(pcm)*2)

	final := s.vosk.AcceptWaveform(bytesSamples)

	partial := s.vosk.PartialResult()
	trimmed := unsafe.Slice(unsafe.StringData(partial), len(partial))
	trimJSON(&trimmed, []byte(`"partial" : "`))

	if len(trimmed) == 0 {
		return
	}

	curStart := *start
	curEnd := *end
	curEstEnd := *estEnd
	curSkip := *skipCnt

	if final == 1 {
		resBuf.Write(trimmed[curStart:])

		s.log.Info("Write FINAL data",
			zap.String("op", op),
			zap.Int("start", curStart),
			zap.Int("end", curEnd))

		*start = 0
		*end = (defaultLength / 4) - trashLen
		*skipCnt = 0

		s.vosk.Reset()
		return
	}

	if len(trimmed) < curStart {
		s.log.Info("Vosk partial shrunk, resetting state",
			zap.String("op", op),
			zap.Int("current length", len(trimmed)),
			zap.Int("curStart", curStart))

		*start = 0
		*end = (defaultLength / 4) - trashLen
		*skipCnt = 0

		return
	}

	if len(trimmed) < curEnd {
		s.log.Warn("Trimmed text is too short",
			zap.String("op", op),
			zap.Int("len", len(trimmed)),
			zap.Int("curEnd", curEnd))
		return
	}

	if curSkip >= 10 {
		curSkip = 0

		localEnd := bytes.IndexByte(trimmed[curEstEnd:], ' ')
		if localEnd == -1 {
			localEnd = curEstEnd
		} else {
			localEnd += curEstEnd
		}
		curEnd = localEnd

		resBuf.Write(trimmed[curStart:curEnd])

		s.log.Info("Write data",
			zap.String("op", op),
			zap.Int("start", curStart),
			zap.Int("end", curEnd))

		curStart = curEnd
		curEstEnd = 0

		curEnd += (defaultLength / 4) - trashLen

	} else if curSkip == 0 {
		curSkip++
		curEstEnd = curEnd
	} else {
		curSkip++
	}

	*start = curStart
	*end = curEnd
	*estEnd = curEstEnd
	*skipCnt = curSkip

	s.log.Info("Skip count",
		zap.String("op", op),
		zap.Int("skipCnt", curSkip))
}

// trimJSON removes the pattern from the JSON
// Used for removing "partial" : "" from the JSON
func trimJSON(d *[]byte, pattern []byte) {
	json := *d

	start := bytes.Index(json, pattern)
	if start == -1 {
		return
	}
	start += len(pattern)

	end := bytes.LastIndexByte(json[start:], '"')
	if end == -1 {
		*d = json[start:]
		return
	}
	end += start

	*d = json[start:end]
}

// bytesToFloat32 converts bytes to float32 slice
func bytesToFloat32(bytes []byte, buf *[]float32) {
	if len(bytes) == 0 {
		return
	}

	b := *buf
	neededLen := len(bytes) / 4
	if cap(b) < neededLen {
		b = make([]float32, neededLen)
	} else {
		b = b[:neededLen]
	}

	for i := range b {
		bits := binary.LittleEndian.Uint32(bytes[i*4:])
		b[i] = math.Float32frombits(bits)
	}
	*buf = b
}

// float32ToVosk converts float32 slice to bytes
func float32ToVosk(pcm []float32, int16Samples []int16) {
	if len(pcm) == 0 {
		return
	}

	for i, f := range pcm {
		if f > 1.0 {
			f = 1.0
		} else if f < -1.0 {
			f = -1.0
		}

		int16Samples[i] = int16(f * 32767.0)
	}
}
