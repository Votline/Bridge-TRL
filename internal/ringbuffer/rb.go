// Package ringbuffer implements a lock-free ring buffer.
// Used for communication between user and translator.
package ringbuffer

import (
	"runtime"
	"sync/atomic"
	"time"
)

// RingBuffer struct with cursors and buffer
type RingBuffer[T any] struct {
	wPos, rPos uint64
	bufSize    uint64
	buf        []T
	isClosed   int32 // 0 - not closed, 1 - closed
}

// NewRB creates a new RingBuffer with a given buffer size.
func NewRB[T any](bufSize uint64) *RingBuffer[T] {
	return &RingBuffer[T]{
		wPos:    0,
		rPos:    0,
		bufSize: bufSize,
		buf:     make([]T, bufSize),
	}
}

// Write writes a float32 slice to the buffer.
// Returns number of float32s written.
func (b *RingBuffer[T]) Write(val []T) int {
	if val == nil {
		return 0
	}

	spinIdx := 0
	for {
		w := atomic.LoadUint64(&b.wPos)
		r := atomic.LoadUint64(&b.rPos)

		available := b.bufSize - (w - r)
		n := min(available, uint64(len(val)))

		if n == 0 {
			Spin(&spinIdx)
			continue
		}

		pos := w % b.bufSize

		if pos+n <= b.bufSize {
			copy(b.buf[pos:], val[:n])
		} else {
			firstPart := b.bufSize - pos
			copy(b.buf[pos:], val[:firstPart])
			copy(b.buf[:n-firstPart], val[firstPart:n])
		}

		atomic.AddUint64(&b.wPos, n)
		return int(n)
	}
}

// Read reads a float32 slice from the buffer.
// Returns number of float32s read.
func (b *RingBuffer[T]) Read(p []T) int {
	spinIdx := 0
	for {
		w := atomic.LoadUint64(&b.wPos)
		r := atomic.LoadUint64(&b.rPos)

		available := w - r
		if available > 0 {
			n := min(uint64(len(p)), available)

			pos := r % b.bufSize

			if pos+n <= b.bufSize {
				copy(p, b.buf[pos:pos+n])
			} else {
				firstPart := b.bufSize - pos
				copy(p[:firstPart], b.buf[pos:])
				copy(p[firstPart:n], b.buf[:n-firstPart])
			}

			atomic.AddUint64(&b.rPos, n)
			return int(n)
		}

		if b.IsClosed() {
			return -1
		}

		Spin(&spinIdx)
	}
}

// ReadAll waits for the buffer to accumulate enough data.
// Then it reads the data from the buffer.
func (b *RingBuffer[T]) ReadAll(p []T, n int) int {
	var available uint64 = 0
	var w, r uint64 = 0, 0
	spinIdx := 0

	reqLen := uint64(n)
	for available < reqLen {
		w = atomic.LoadUint64(&b.wPos)
		r = atomic.LoadUint64(&b.rPos)
		available = w - r
		Spin(&spinIdx)
	}

	toRead := reqLen

	for i := range toRead {
		p[i] = b.buf[(r+i)%b.bufSize]
	}

	atomic.AddUint64(&b.rPos, toRead)

	return int(toRead)
}

// Spin is a helper function for spinning.
func Spin(idx *int) {
	*idx++
	if *idx < 10 {
		runtime.Gosched()
	} else {
		time.Sleep(time.Millisecond)
		*idx = 0
	}
}

// Reset resets the buffer.
func (b *RingBuffer[T]) Reset() {
	atomic.StoreUint64(&b.wPos, 0)
	atomic.StoreUint64(&b.rPos, 0)
}

// Len returns the current length of the buffer.
func (b *RingBuffer[T]) Len() int {
	return int(atomic.LoadUint64(&b.wPos) - atomic.LoadUint64(&b.rPos))
}

// Close the buffer.
func (b *RingBuffer[T]) Close() {
	atomic.StoreInt32(&b.isClosed, 1)
}

// IsClosed returns true if the buffer is closed.
func (b *RingBuffer[T]) IsClosed() bool {
	return atomic.LoadInt32(&b.isClosed) == 1
}

// Open the buffer.
func (b *RingBuffer[T]) Open() {
	atomic.StoreInt32(&b.isClosed, 0)
}
