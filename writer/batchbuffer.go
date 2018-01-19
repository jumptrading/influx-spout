package writer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"
)

func newBatchBuffer() *batchBuffer {
	b := &batchBuffer{
		buf: new(bytes.Buffer),
	}
	b.buf.Grow(32 * os.Getpagesize())
	return b
}

type batchBuffer struct {
	buf     *bytes.Buffer
	created time.Time
	writes  int
}

func (b *batchBuffer) Write(data []byte) error {
	// Set the creation timestamp on the first write.
	if b.writes == 0 {
		b.created = time.Now()
	}
	b.writes++
	if n, err := b.buf.Write(data); err != nil {
		return fmt.Errorf("failed to write to message buffer (wrote %d): %v\n", n, err)
	}
	return nil
}

func (b *batchBuffer) Data() io.Reader {
	return b.buf
}

func (b *batchBuffer) Writes() int {
	return b.writes
}

func (b *batchBuffer) Size() int {
	return b.buf.Len()
}

func (b *batchBuffer) Age() time.Duration {
	if b.writes == 0 {
		return time.Duration(0)
	}
	return time.Since(b.created)
}

func (b *batchBuffer) Reset() {
	b.buf.Reset()
	b.writes = 0
}
