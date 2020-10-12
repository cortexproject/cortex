package scheduler

import (
	"fmt"
	"time"
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
	PlanFiles     []string
	ProgressFiles map[string]time.Time
	Finished      []string
	ErrorFile     string
}

func (ps plan) Status() planStatus {
	if len(ps.PlanFiles) != 1 || len(ps.Finished) > 1 || (len(ps.Finished) > 0 && ps.ErrorFile != "") {
		return Invalid
	}

	if len(ps.Finished) > 0 {
		return Finished
	}

	if ps.ErrorFile != "" {
		return Error
	}

	if len(ps.ProgressFiles) > 0 {
		return InProgress
	}

	return New
}
