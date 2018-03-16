// Provide an "event" interface for observability

// Temporary hack implementation to go via logger to stderr
package util

import (
	"os"

	"github.com/go-kit/kit/log"
)

var (
	// interface{} vars to avoid allocation on every call
	key   interface{} = "level" // masquerade as a level like debug, warn
	event interface{} = "event"

	eventLogger log.Logger = log.NewNopLogger()
)

func Event() log.Logger {
	return eventLogger
}

func InitEvents(freq int) {
	if freq <= 0 {
		eventLogger = log.NewNopLogger()
	} else {
		eventLogger = newEventLogger(freq)
	}
}

func newEventLogger(freq int) log.Logger {
	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.WithPrefix(l, key, event)
	l = log.With(l, "ts", log.DefaultTimestampUTC)
	return &samplingFilter{next: l, freq: freq}
}

type samplingFilter struct {
	next  log.Logger
	freq  int
	count int
}

func (e *samplingFilter) Log(keyvals ...interface{}) error {
	e.count++
	if e.count%e.freq == 0 {
		return e.next.Log(keyvals...)
	}
	return nil
}
