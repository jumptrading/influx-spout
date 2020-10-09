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

package batchsplitter_test

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/jumptrading/influx-spout/v2/batchsplitter"
	"github.com/stretchr/testify/assert"
)

func TestEmpty(t *testing.T) {
	b := []byte("")
	splitter := batchsplitter.New(b, 100*datasize.B)

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestNoSplit(t *testing.T) {
	b := []byte("abcdefghij\n")
	splitter := batchsplitter.New(b, 100*datasize.B)

	assert.True(t, splitter.Next())
	assert.Equal(t, b, splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestNoSplitWithLines(t *testing.T) {
	b := []byte("abcd\nefg\nhij")
	splitter := batchsplitter.New(b, 100*datasize.B)

	assert.True(t, splitter.Next())
	assert.Equal(t, b, splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestNoSplitExact(t *testing.T) {
	b := []byte("abcd\nefg\nhij")
	splitter := batchsplitter.New(b, datasize.ByteSize(len(b)))

	assert.True(t, splitter.Next())
	assert.Equal(t, b, splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestChunks(t *testing.T) {
	b := []byte("1111\n2222\n333\n")
	splitter := batchsplitter.New(b, 6*datasize.B)

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("1111\n"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("2222\n"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("333\n"), splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestChunksExact(t *testing.T) {
	b := []byte("1111\n2222\n3333\n")
	splitter := batchsplitter.New(b, 5*datasize.B)

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("1111\n"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("2222\n"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("3333\n"), splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestLineTooLong(t *testing.T) {
	b := []byte("01234567")
	splitter := batchsplitter.New(b, 3*datasize.B)

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("012"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("345"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("67"), splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}

func TestMultiLineTooLong(t *testing.T) {
	b := []byte("0123456\n88\n99")
	splitter := batchsplitter.New(b, 3*datasize.B)

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("012"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("345"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("6\n"), splitter.Chunk())

	// Test recovery.
	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("88\n"), splitter.Chunk())

	assert.True(t, splitter.Next())
	assert.Equal(t, []byte("99"), splitter.Chunk())

	assert.False(t, splitter.Next())
	assert.Nil(t, splitter.Chunk())
}
