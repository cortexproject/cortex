package configs

import (
	"errors"
	"flag"
	"strings"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/resource"
)

type InstanceLimits struct {
	CPUUtilization  float64 `yaml:"cpu_utilization"`
	HeapUtilization float64 `yaml:"heap_utilization"`
}

func (cfg *InstanceLimits) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Float64Var(&cfg.CPUUtilization, prefix+"instance-limits.cpu-utilization", 0, "Max CPU utilization that this ingester can reach before rejecting new query request (across all tenants) in percentage, between 0 and 1. 0 = unlimited.")
	f.Float64Var(&cfg.HeapUtilization, prefix+"instance-limits.heap-utilization", 0, "Max heap utilization that this ingester can reach before rejecting new query request (across all tenants) in percentage, between 0 and 1. 0 = unlimited.")
}

func (cfg *InstanceLimits) Validate(monitoredResources flagext.StringSliceCSV) error {
	if cfg.CPUUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.CPU)) {
		return errors.New("monitored_resources config must include \"cpu\" as well")
	}

	if cfg.HeapUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.Heap)) {
		return errors.New("monitored_resources config must include \"heap\" as well")
	}

	return nil
}
