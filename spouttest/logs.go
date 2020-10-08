package spouttest

import (
	"io/ioutil"
	"log"
	"os"
)

// SuppressLogs causes log output globally to be discarded.
func SuppressLogs() {
	log.SetOutput(ioutil.Discard)
}

// RestoreLogs causes log output globally to be sent to stderr.
func RestoreLogs() {
	log.SetOutput(os.Stderr)
}
