package scheduler

import (
	"fmt"
	"time"

	"github.com/oklog/ulid"
)

type planStatus int

const (
	New planStatus = iota
	InProgress
	Finished
	Error
	Invalid
)

func (s planStatus) String() string {
	switch s {
	case New:
		return "New"
	case InProgress:
		return "InProgress"
	case Finished:
		return "Finished"
	case Error:
		return "Error"
	case Invalid:
		return "Invalid"
	default:
		panic(fmt.Sprintf("invalid status: %d", s))
	}
}

type plan struct {
	planFile      string
	progressFiles map[string]time.Time
	finished      []ulid.ULID
	errorFile     bool
}

func (ps plan) Status() planStatus {
	if ps.planFile == "" || len(ps.finished) > 0 || (len(ps.finished) > 0 && ps.errorFile) {
		return Invalid
	}

	if len(ps.finished) > 0 {
		return Finished
	}

	if ps.errorFile {
		return Error
	}

	if len(ps.progressFiles) > 0 {
		return InProgress
	}

	return New
}
