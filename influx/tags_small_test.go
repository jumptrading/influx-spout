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

package influx

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTags(t *testing.T) {
	check := func(line string, em string, et TagSet, er string) {
		am, at, ar, err := ParseTags([]byte(line))
		assert.NoError(t, err)
		assert.Equal(t, em, string(am), "wrong measurement, ParseTags(%q)", line)
		assert.Equal(t, et, at, "wrong tags, ParseTags(%q)", line)
		assert.Equal(t, er, string(ar), "wrong remainder, ParseTags(%q)", line)
	}
	checkError := func(line string, errMsg string) {
		_, _, _, err := ParseTags([]byte(line))
		assert.EqualError(t, err, errMsg)
	}

	check(``, "", nil, "")

	// Just a measurement (unlikely)
	check(`foo`, "foo", nil, "")

	//No tags
	check(`foo x=22`, "foo", nil, "x=22")
	check(`foo x=22 y=true`, "foo", nil, "x=22 y=true")

	// Single tag
	check(`foo,host=s1 x=22`,
		"foo",
		TagSet{NewTag("host", "s1")},
		"x=22",
	)

	// Multiple tags
	check(`foo,host=s1,dc=nyc abc=22i xyz=3.141`,
		"foo",
		TagSet{
			NewTag("host", "s1"),
			NewTag("dc", "nyc"),
		},
		"abc=22i xyz=3.141",
	)

	// Tags without fields (unlikely)
	check(`foo,host=s1,dc=nyc`,
		"foo",
		TagSet{
			NewTag("host", "s1"),
			NewTag("dc", "nyc"),
		},
		"",
	)

	// Escaping horror.
	check(`fo\,o,h\=ost=s\,1,\,dc=\ nyc abc=22i xyz=3.141`,
		"fo,o",
		TagSet{
			NewTag("h=ost", "s,1"),
			NewTag(",dc", " nyc"),
		},
		"abc=22i xyz=3.141",
	)

	checkError(`foo,dc`, "invalid tag")
	checkError(`foo,dc x=22`, "invalid tag")
	checkError(`foo,dc,host=s1 x=22`, "invalid tag")
	checkError(`foo,host=s1,dc x=22`, "invalid tag")
	checkError(`foo,dc=`, "invalid tag")
}

func TestTagSetSorting(t *testing.T) {
	tags := TagSet{
		NewTag("foo", "0"),
		NewTag("zeal", "2"),
		NewTag("longer", "1"),
		NewTag("bar", "3"),
	}

	sort.Sort(tags)

	expected := TagSet{
		NewTag("bar", "3"),
		NewTag("foo", "0"),
		NewTag("longer", "1"),
		NewTag("zeal", "2"),
	}
	assert.Equal(t, expected, tags)
}

func TestSubsetOf(t *testing.T) {
	tags := TagSet{
		NewTag("foo", "0"),
		NewTag("bar", "1"),
		NewTag("sne", "2"),
	}

	// Equal sets.
	assert.True(t, tags.SubsetOf(TagSet{
		NewTag("sne", "2"),
		NewTag("bar", "1"),
		NewTag("foo", "0"),
	}))

	// Subset with extras.
	assert.True(t, tags.SubsetOf(TagSet{
		NewTag("sne", "2"),
		NewTag("a", "3"),
		NewTag("bar", "1"),
		NewTag("foo", "0"),
		NewTag("zzzzz", "4"),
	}))

	// Partial match (not sufficient).
	assert.False(t, tags.SubsetOf(TagSet{
		NewTag("sne", "2"),
		NewTag("foo", "0"),
	}))

	// No overlap.
	assert.False(t, tags.SubsetOf(TagSet{
		NewTag("q", "0"),
	}))

	// Empty.
	assert.False(t, tags.SubsetOf(nil))

	// Empty source is always a subset of anything else.
	assert.True(t, TagSet{}.SubsetOf(TagSet{NewTag("foo", "bar")}))
}

func TestBytes(t *testing.T) {
	check := func(input TagSet, expected string) {
		actual := input.Bytes()
		assert.Equal(t, expected, string(actual), "input: %v", input)
	}

	// One tag.
	check(TagSet{NewTag("foo", "bar")}, "foo=bar")

	// Multiple tags.
	check(
		TagSet{
			NewTag("foo", "0"),
			NewTag("bar", "1"),
			NewTag("sne", "2"),
		},
		"foo=0,bar=1,sne=2",
	)

	// Escaping.
	check(
		TagSet{
			NewTag("foo=bar", "0"),
			NewTag("ba,r", "1"),
			NewTag("host", "a server"),
		},
		`foo\=bar=0,ba\,r=1,host=a\ server`,
	)

	// Empty.
	check(TagSet{}, "")
}
