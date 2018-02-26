package spouttest

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// AssertRecv checks that a specific string has been received from a
// channel. The check times out after LongWait.
func AssertRecv(t *testing.T, ch <-chan string, label, expected string) {
	expected = stripLeadingNL(expected)

	select {
	case received := <-ch:
		assert.Equal(t, expected, received)
	case <-time.After(LongWait):
		t.Fatalf("timed out waiting for %s", label)
	}
}

// AssertRecvMulti checks that a specific string has been received
// from a channel. The channel will be read multiple times if
// required. The check times out after LongWait.
func AssertRecvMulti(t *testing.T, ch <-chan string, label, expected string) {
	expected = stripLeadingNL(expected)

	var received string
	timeout := time.After(LongWait)
	for {
		select {
		case received = <-ch:
			if expected == received {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s. last received: %q", label, received)
		}
	}
}

func stripLeadingNL(s string) string {
	// This allows long `expected` strings to be formatted nicely in
	// the caller.
	if strings.HasPrefix(s, "\n") {
		return s[1:]
	}
	return s
}
