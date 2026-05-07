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

	rec.SetMaxAlternatives(0)

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

	// audioBuf used for receiving audio data from WS message
	audioBuf := gd.NewRingBuffer(defAudioLen)

	// gotPCM used for converting audio data to float32 slice
	gotPCMPtr := audioBufPool.Get().(*[]float32)
	gotPCM := (*gotPCMPtr)[:0]
	defer audioBufPool.Put(gotPCMPtr)

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

				bytesToFloat32(msg, &gotPCM)
				audioBuf.Write(gotPCM)
			}
		}
	})

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
	end := (defaultLength / 4) - trashLen
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

				if len(sendPCM) < audioBuf.Len() {
					sendPCM = make([]float32, audioBuf.Len())
				}

				cut := audioBuf.Read(sendPCM)
				pcm := sendPCM[:cut]

				t.processAudio(pcm, resBuf, int16Smp, &start, &end)
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
				temp := res[:cut]

				if err := conn.WriteMessage(websocket.TextMessage, temp); err != nil {
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

// processAudio get text from audio
func (t *STT) processAudio(pcm []float32, resBuf *rb.RingBuffer[byte], int16Samples []int16, start, end *int) {
	const op = "workers.STT.processAudio"

	float32ToVosk(pcm, int16Samples)
	bytesSamples := unsafe.Slice((*byte)(unsafe.Pointer(&int16Samples[0])), len(pcm)*2)

	curEnd := *end
	curStart := *start

	if t.vosk.AcceptWaveform(bytesSamples) != 0 {
		res := t.vosk.FinalResult()
		full := unsafe.Slice(unsafe.StringData(res), len(res))
		trimJSON(&full, []byte(`"text" : "`))

		if len(full) > 0 {
			resBuf.Write(full)
		}

		*start = 0
		*end = (defaultLength / 4) - trashLen
	} else {
		partial := t.vosk.PartialResult()
		full := unsafe.Slice(unsafe.StringData(partial), len(partial))
		trimJSON(&full, []byte(`"partial" : "`))

		winSize := (defaultLength / 4) - trashLen
		if len(full) > curStart {
			if curEnd > len(full) || curEnd == 0 {
				curEnd = len(full)
			}

			if curEnd > curStart {
				win := full[curStart:curEnd]
				resBuf.Write(win)
				curStart += len(win)
				curEnd += len(win)
			}
		}
		t.log.Info("Partial result",
			zap.String("op", op),
			zap.Int("len", len(partial)),
			zap.Int("curStart", curStart),
			zap.Int("curEnd", curEnd),
			zap.Int("winSize", winSize))
	}

	*end = curEnd
	*start = curStart
}

// trimJSON remove  pattern from the VOSK resutl
func trimJSON(d *[]byte, pattern []byte) {
	json := *d

	start := bytes.Index(json, pattern)
	if start == -1 {
		return
	}
	start += len(pattern)

	relativeEnd := bytes.IndexByte(json[start:], '"')
	if relativeEnd == -1 {
		*d = json[start:]
		return
	}

	actualEnd := start + relativeEnd

	*d = json[start:actualEnd]
}
