package spouttest

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func stripLeadingNL(s string) string {
	// This allows long `expected` strings to be formatted nicely in
	// the caller.
	if strings.HasPrefix(s, "\n") {
		return s[1:]
	}
	return s
}

// AssertMonitor ensures that a number of lines have been from a
// component's statistician goroutine. The target lines may arrive in
// any order and non-matching lines are ignored. Timestamps on the
// received lines are checked for and then stripped.
func AssertMonitor(t *testing.T, ch chan string, expected []string) {
	remaining := make(map[string]bool)
	for _, line := range expected {
		remaining[line] = true
	}

	var seenLines string
	timeout := time.After(LongWait)
	for {
		select {
		case lines := <-ch:
			for _, line := range strings.Split(lines, "\n") {
				if len(line) == 0 {
					continue
				}
				line = stripTimestamp(t, line)
				seenLines += fmt.Sprintf("%s\n", line)
				delete(remaining, line)
			}
			if len(remaining) < 1 {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for expected lines. expected:\n%s\nsaw:\n%s",
				strings.Join(expected, "\n"),
				seenLines,
			)
		}
	}
}

// StripTimestamps takes a string containing one or more metrics
// lines, validates that each line appears to end with a timestamp and
// then strips the timestamp off. The returned string is the same as
// the input but without the timestamps (for easier test comparisons).
func StripTimestamps(t *testing.T, s string) string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		out = append(out, stripTimestamp(t, line))
	}
	return strings.Join(out, "\n")
}

func stripTimestamp(t *testing.T, s string) string {
	if len(s) < 1 {
		return ""
	}

	i := strings.LastIndexByte(s, ' ')
	require.True(t, i >= 0)

	// Check that end looks like a timestamp
	_, err := strconv.Atoi(s[i+1:])
	require.NoError(t, err)

	// Strip off the timestamp
	return s[:i]
}

// CheckReadyProbe repeatedly tries a readiness probe on the given
// port. It will return true if a successful readiness probe is
// observed within LongWait.
func CheckReadyProbe(probePort int) bool {
	if probePort <= 0 {
		panic("probe port must be greater than 0")
	}
	maxTime := time.Now().Add(LongWait)

	url := fmt.Sprintf("http://localhost:%d/readyz", probePort)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(maxTime) {
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == 200 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}

	return false
}

// AssertReadyProbe fails a test if CheckReadyProbe returns false.
func AssertReadyProbe(t require.TestingT, probePort int) {
	if CheckReadyProbe(probePort) {
		return
	}
	t.Errorf("timed out waiting for successful ready probe")
	t.FailNow()
}
