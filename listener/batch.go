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

package listener

import (
	"io"
)

// newBatch returns a new batch buffer with the initial capacity
// specified.
func newBatch(capacity int) *batch {
	return &batch{
		buf: make([]byte, 0, capacity),
	}
}

// batch implements a fixed buffer for storing a listener's current
// batch. It is structured to minimise allocations and copies. Bytes
// are read from an io.Reader (typically a network connection)
// directly into an internal preallocated byte slice.
//
// Some ideas are borrowed from bytes.Buffer - the main difference is
// the readOnceFrom method which reads just once from an
// io.Reader. This is required to avoid grouping UDP reads together.
type batch struct {
	buf []byte
}

// readOnceFrom reads into the batch just once from an io.Reader.
func (b *batch) readOnceFrom(r io.Reader) (int, error) {
	n, err := r.Read(b.buf[len(b.buf):cap(b.buf)])
	if n > 0 {
		b.buf = b.buf[:len(b.buf)+n]
	}
	return n, err
}

// readFrom reads everything from an io.Reader, growing the batch if
// required.
func (b *batch) readFrom(r io.Reader) (int, error) {
	total := 0
	for {
		// If there's not much capacity left, grow the buffer.
		if b.remaining() <= 512 {
			b.grow()
		}
		n, err := r.Read(b.buf[len(b.buf):cap(b.buf)])
		if n > 0 {
			b.buf = b.buf[:len(b.buf)+n]
			total += n
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return total, err
		}
	}
}

// appendBytes adds some bytes to the batch, growing the batch if
// required.
func (b *batch) appendBytes(more []byte) {
	lenMore := len(more)
	for b.remaining() < lenMore {
		b.grow()
	}

	lenBatch := len(b.buf)
	b.buf = b.buf[:lenBatch+lenMore]
	copy(b.buf[lenBatch:], more)
}

// size returns the number of bytes currently stored in the batch.
func (b *batch) size() int {
	return len(b.buf)
}

// remaining returns the number of bytes still unused in the batch.
func (b *batch) remaining() int {
	return cap(b.buf) - len(b.buf)
}

// reset clears the batch so that it no longer holds data.
func (b *batch) reset() {
	b.buf = b.buf[:0]
}

// bytes returns the underlying batch byte slice. The returned slice
// is valid only until the next modifying call to the batch.
func (b *batch) bytes() []byte {
	return b.buf
}

// grow doubles the size of the batch's internal buffer. This is
// expensive and should be avoided where possible.
func (b *batch) grow() {
	newBuf := make([]byte, int(cap(b.buf)*2))
	copy(newBuf, b.buf)
	newBuf = newBuf[:len(b.buf)]
	b.buf = newBuf
}
