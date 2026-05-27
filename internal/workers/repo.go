// Package workers contains workers interface.
package workers

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"unsafe"

	rb "btrl/internal/ringbuffer"

	"go.uber.org/zap"
)

const (
	defaultLength = 512
	defaultRBLen  = defaultLength
	defAudioLen   = 4096
	defaultDicts  = "dicts/"
)

// bufPool is a pool for buffers
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, defaultLength)
		return &b
	},
}

var ringBufPool = sync.Pool{
	New: func() any {
		b := rb.NewRB[byte](defaultRBLen)
		return b
	},
}

var audioBufPool = sync.Pool{
	New: func() any {
		b := make([]float32, defAudioLen)
		return &b
	},
}

var int16AudioBufPool = sync.Pool{
	New: func() any {
		b := make([]int16, defAudioLen)
		return &b
	},
}

// Worker is an interface for workers.
// You can use it for custom endpoints.
type Worker interface {
	GetName() string
	Register(m *http.ServeMux)
}

func findAPIPrefix(d string) bool {
	data := unsafe.Slice(unsafe.StringData(d), len(d))

	has := bytes.Index(data, []byte("http"))
	if has == -1 {
		has = bytes.Index(data, []byte("https"))
		if has == -1 {
			return false
		}
	}
	return true
}

func callScript(scriptCall string, jsonData []byte, log *zap.Logger) ([]byte, error) {
	const op = "workers.repo.callScript"

	log.Info("Call script",
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
