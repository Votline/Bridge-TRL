// Package workers stt.go contains worker implementations for speech to text
// Speech to text
// Returned text is in the source language
package workers

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
	"unsafe"

	rb "btrl/internal/ringbuffer"

	gd "github.com/Votline/Go-audio"
	"github.com/alphacep/vosk-api/go"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const sampleRateSTT = 16000.0

// STT struct for implementing worker
// Contains 'stt' endpoint for speech to text
type STT struct {
	// Name of the worker
	Name string

	// vosk is a recognizer for speech to text
	vosk *vosk.VoskRecognizer

	log    *zap.Logger
	upg    websocket.Upgrader
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSTT creates a new STT worker
func NewSTT(log *zap.Logger) (*STT, error) {
	const op = "workers.NewSTT"

	upg := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	model, err := vosk.NewModel("assets/vosk-model-small-ru-0.22")
	if err != nil {
		return nil, fmt.Errorf("%s: new vosk model: %w", op, err)
	}

	rec, err := vosk.NewRecognizer(model, sampleRateSTT)
	if err != nil {
		return nil, fmt.Errorf("%s: new vosk recognizer: %w", op, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &STT{
		Name:   "STT",
		vosk:   rec,
		log:    log,
		upg:    upg,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// GetName returns the name of the worker
// Used for logging
func (t *STT) GetName() string {
	return t.Name
}

// Register the worker endpoints on the http.ServeMux
func (t *STT) Register(m *http.ServeMux) {
	m.HandleFunc("/stt", t.STT)
}

// Close cancels the context
// Cancel context is used for shutdown WS connections
func (t *STT) Close(ctx context.Context) {
	t.cancel()
	if t.vosk != nil {
		t.vosk.Free()
	}
}

// STT speech to text
// Use WebSockets for streaming
// Returned text is in the source language
func (t *STT) STT(w http.ResponseWriter, r *http.Request) {
	const op = "workers.STT"

	t.log.Info("STT request",
		zap.String("op", op))

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

	t.log.Info("Upgraded connection",
		zap.String("op", op))

	audioBuf := gd.NewRingBuffer(defAudioLen)

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

				t.log.Info("Got PCM",
					zap.String("op", op),
					zap.Int("len", len(msg)))

				pcm := bytesToFloat32(msg)
				audioBuf.Write(pcm)
			}
		}
	})

	resBuf := rb.NewRB[byte](defaultLength / 2)
	res := make([]byte, defaultLength/4)

	wg.Go(func() {
		for {
			select {
			case <-t.ctx.Done():
				t.log.Info("Context done", zap.String("op", op))
				return
			default:
				if audioBuf.Len() == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				temp := make([]float32, audioBuf.Len())
				cut := audioBuf.Read(temp)
				pcm := temp[:cut]

				t.processAudio(pcm, resBuf)
			}
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-t.ctx.Done():
				t.log.Info("Context done", zap.String("op", op))
				return
			default:
				cut := resBuf.ReadAll(res, defaultLength/4)
				res = res[:cut]

				if err := conn.WriteMessage(websocket.TextMessage, res); err != nil {
					t.log.Error("Failed to write message",
						zap.String("op", op),
						zap.Error(err))
					return
				}

				t.log.Info("Sent message",
					zap.String("op", op),
					zap.Int("len", len(res)))
			}
		}
	})

	wg.Wait()
}

func bytesToFloat32(bytes []byte) []float32 {
	samples := make([]float32, len(bytes)/4)
	for i := range samples {
		bits := binary.LittleEndian.Uint32(bytes[i*4:])
		samples[i] = math.Float32frombits(bits)
	}
	return samples
}

func (t *STT) processAudio(pcm []float32, resBuf *rb.RingBuffer[byte]) {
	const op = "workers.STT.processAudio"

	bytesSamples := float32ToVosk(pcm)

	if t.vosk.AcceptWaveform(bytesSamples) != 0 {
		res, err := parseJSON(t.vosk.Result())
		if err != nil {
			t.log.Error("Failed to parse JSON",
				zap.String("op", op),
				zap.Error(err))
			return
		}
		resBuf.Write(res)
	} else {
		partial := t.vosk.PartialResult()
		t.log.Debug("Partial result",
			zap.String("op", op),
			zap.Int("text", len(partial)))
		if len(partial) > defaultLength/4 {
			res, err := parseJSON(t.vosk.FinalResult())
			if err != nil {
				t.log.Error("Failed to parse JSON",
					zap.String("op", op),
					zap.Error(err))
				return
			}
			resBuf.Write(res)
			t.vosk.Reset()
		}
	}
}

func float32ToVosk(pcm []float32) []byte {
	int16Sampples := make([]int16, len(pcm))

	for i, f := range pcm {
		if f > 1.0 {
			f = 1.0
		} else if f < -1.0 {
			f = -1.0
		}

		int16Sampples[i] = int16(f * 32767.0)
	}

	bytesSamples := unsafe.Slice((*byte)(unsafe.Pointer(&int16Sampples[0])), len(int16Sampples)*2)

	return bytesSamples
}

func parseJSON(d string) ([]byte, error) {
	const op = "workers.parseJSON"

	json := unsafe.Slice(unsafe.StringData(d), len(d))

	res := make([]byte, len(json))

	sep := []byte(`"text" : "`)
	start := bytes.Index(json, sep)
	if start == -1 {
		return nil, fmt.Errorf("%s: failed to find start of text", op)
	}
	start += len(sep)

	end := bytes.Index(json[start:], []byte(`"`))
	if end == -1 {
		return nil, fmt.Errorf("%s: failed to find end of text", op)
	}
	end += start

	copy(res, json[start:end])

	return res, nil
}
