//go:build darwin

package resource

import (
	"github.com/go-kit/log/level"

	"github.com/cortexproject/cortex/pkg/util/log"
)

func newCPUScanner() (scanner, error) {
	level.Warn(log.Logger).Log("msg", "CPU scanner not supported in darwin.")
	return &noopScanner{}, nil
}
