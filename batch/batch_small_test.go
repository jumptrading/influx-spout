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

// +build small

package batch_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/batch"
)

func TestNew(t *testing.T) {
	b := batch.New(64)

	assert.Equal(t, 0, b.Size())
	assert.Equal(t, 64, b.Remaining())
	assert.Equal(t, []byte{}, b.Bytes())
}

func TestAppend(t *testing.T) {
	b := batch.New(10)

	b.Append([]byte("foo"))
	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 7, b.Remaining())
	assert.Equal(t, []byte("foo"), b.Bytes())

	b.Append([]byte("bar"))
	assert.Equal(t, 6, b.Size())
	assert.Equal(t, 4, b.Remaining())
	assert.Equal(t, []byte("foobar"), b.Bytes())
}

func TestAppendGrow(t *testing.T) {
	b := batch.New(2) // only 2 bytes!

	b.Append([]byte("foo")) // add 3 bytes of data to cause growth
	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 1, b.Remaining())
	assert.Equal(t, []byte("foo"), b.Bytes())

	b.Append([]byte("bar")) // add another 3 bytes of data to cause growth again
	assert.Equal(t, 6, b.Size())
	assert.Equal(t, 2, b.Remaining())
	assert.Equal(t, []byte("foobar"), b.Bytes())
}

func TestReadFrom(t *testing.T) {
	b := batch.New(10)
	r := bytes.NewReader([]byte("foo"))
	count, err := b.ReadFrom(r)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 37, b.Remaining())
	assert.Equal(t, []byte("foo"), b.Bytes())
}

func TestReadOnceFrom(t *testing.T) {
	b := batch.New(10)
	r := bytes.NewReader([]byte("foo"))
	count, err := b.ReadOnceFrom(r)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 17, b.Remaining())
	assert.Equal(t, []byte("foo"), b.Bytes())
}

func TestReset(t *testing.T) {
	b := batch.New(10)
	b.Append([]byte("foo"))
	assert.Equal(t, []byte("foo"), b.Bytes())

	b.Reset()
	assert.Equal(t, 0, b.Size())
	assert.Equal(t, 10, b.Remaining())
	assert.Equal(t, []byte{}, b.Bytes())
}
