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

package batchsplitter

// New returns a BatchSplitter which will efficiently
// split BUF into chunks no larger than CHUNKSIZE with splits occuring
// on newline boundaries.
//
// Use repeated calls to Next to generate the splits.
//
// Under no circumstances will chunks of more than chunkSize be
// returned. In the unlikely case of a line existing that is larger
// than chunkSize, it will be broken up.
func New(buf []byte, chunkSize int) *BatchSplitter {
	return &BatchSplitter{
		buf:       buf,
		chunkSize: chunkSize,
	}
}

// BatchSplitter which will efficiently split a byte slice into chunks
// of no larger than some size with splits occuring on newline
// boundaries.
type BatchSplitter struct {
	buf       []byte
	chunkSize int
	out       []byte
}

// Next scans for the next chunk, returning true if there was another
// chunk to return. Use Chunk() to retrieve the bytes for the chunk.
func (s *BatchSplitter) Next() bool {
	if len(s.buf) == 0 {
		s.out = nil
		return false
	}
	if len(s.buf) <= s.chunkSize {
		s.out = s.buf
		s.buf = nil
		return true
	}

	// Look backwards from the maximum size for a newline.
	for i := s.chunkSize; i >= 0; i-- {
		if s.buf[i] == '\n' {
			i++
			s.out = s.buf[:i]
			s.buf = s.buf[i:]
			return true
		}
	}

	// No newline found, which means that a line was bigger than
	// chunkSize. This is bad (but highly unlikely in practice). Split
	// up the line at chunkSize.
	s.out = s.buf[:s.chunkSize]
	s.buf = s.buf[s.chunkSize:]
	return true
}

// Chunk returns the chunk found by the last call to Next(). The
// returned slice is only useful until the next call to Next().
func (s *BatchSplitter) Chunk() []byte {
	return s.out
}
