// Copyright 2018 Jump Trading
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package batch implements a type storing a batch of measurement lines.
package batch

import (
	"io"
)

const minReadSize = 1024

// New returns a new batch buffer with the initial capacity
// specified (in bytes).
func New(capacity int) *Batch {
	return &Batch{
		buf: make([]byte, 0, capacity),
	}
}

// Batch implements a fixed buffer of bytes. It is structured to
// minimise allocations and copies. Bytes can be directly read from an
// io.Reader (typically a network connection) directly into the
// internal preallocated byte slice.
//
// Some ideas are borrowed from bytes.Buffer. One difference is the
// ReadOnceFrom method which reads just once from an io.Reader. This
// is required to avoid grouping UDP reads together.
type Batch struct {
	buf []byte
}

// Size returns the number of bytes currently stored in the Batch.
func (b *Batch) Size() int {
	return len(b.buf)
}

// Remaining returns the number of bytes still unused in the Batch.
func (b *Batch) Remaining() int {
	return cap(b.buf) - len(b.buf)
}

// Bytes returns the underlying Batch byte slice. The returned slice
// is valid only until the next modifying call to the Batch.
func (b *Batch) Bytes() []byte {
	return b.buf
}

// Reset clears the Batch so that it no longer holds data.
func (b *Batch) Reset() {
	b.buf = b.buf[:0]
}

// Append adds some bytes to the Batch, growing the Batch if required.
func (b *Batch) Append(more []byte) {
	lenMore := len(more)
	for b.Remaining() < lenMore {
		b.grow()
	}

	lenBatch := len(b.buf)
	b.buf = b.buf[:lenBatch+lenMore]
	copy(b.buf[lenBatch:], more)
}

// ReadFrom reads everything from an io.Reader, growing the Batch if
// required.
func (b *Batch) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	for {
		// If there's not much capacity left, grow the buffer.
		if b.Remaining() <= minReadSize {
			b.grow()
		}
		n, err := r.Read(b.buf[len(b.buf):cap(b.buf)])
		if n > 0 {
			b.buf = b.buf[:len(b.buf)+n]
			total += int64(n)
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return total, err
		}
	}
}

// ReadOnceFrom reads into the Batch just once from an io.Reader.
func (b *Batch) ReadOnceFrom(r io.Reader) (int, error) {
	// If there's not much capacity left, grow the buffer.
	if b.Remaining() <= minReadSize {
		b.grow()
	}

	n, err := r.Read(b.buf[len(b.buf):cap(b.buf)])
	if n > 0 {
		b.buf = b.buf[:len(b.buf)+n]
	}
	return n, err
}

// grow doubles the size of the Batch's internal buffer. This is
// expensive and should be avoided where possible.
func (b *Batch) grow() {
	newBuf := make([]byte, int(cap(b.buf)*2))
	copy(newBuf, b.buf)
	newBuf = newBuf[:len(b.buf)]
	b.buf = newBuf
}
