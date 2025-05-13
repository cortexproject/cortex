//go:build linux

package resource

import (
	"github.com/pkg/errors"
	"github.com/prometheus/procfs"
)

type cpuScanner struct {
	proc procfs.Proc
}

func newCPUScanner() (scanner, error) {
	proc, err := procfs.Self()
	if err != nil {
		return nil, errors.Wrap(err, "error reading proc directory")
	}

	return &cpuScanner{proc: proc}, nil
}

func (s *cpuScanner) scan() (float64, error) {
	stat, err := s.proc.Stat()
	if err != nil {
		return 0, err
	}

	return stat.CPUTime(), nil
}
