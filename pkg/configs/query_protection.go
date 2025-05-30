package configs

import (
	"errors"
	"flag"
	"strings"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/resource"
)

type QueryProtection struct {
	Rejection rejection `json:"rejection"`
}

type rejection struct {
	Enabled   bool      `yaml:"enabled"`
	Threshold threshold `yaml:"threshold"`
}

type threshold struct {
	CPUUtilization  float64 `yaml:"cpu_utilization"`
	HeapUtilization float64 `yaml:"heap_utilization"`
}

func (cfg *QueryProtection) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Rejection.Enabled, prefix+"query-protection.rejection.enabled", false, "EXPERIMENTAL: Enable query rejection feature, where the component return 503 to all incoming query requests when the configured thresholds are breached.")
	f.Float64Var(&cfg.Rejection.Threshold.CPUUtilization, prefix+"query-protection.rejection.threshold.cpu-utilization", 0, "EXPERIMENTAL: Max CPU utilization that this ingester can reach before rejecting new query request (across all tenants) in percentage, between 0 and 1. monitored_resources config must include the resource type. 0 to disable.")
	f.Float64Var(&cfg.Rejection.Threshold.HeapUtilization, prefix+"query-protection.rejection.threshold.heap-utilization", 0, "EXPERIMENTAL: Max heap utilization that this ingester can reach before rejecting new query request (across all tenants) in percentage, between 0 and 1. monitored_resources config must include the resource type. 0 to disable.")
}

func (cfg *QueryProtection) Validate(monitoredResources flagext.StringSliceCSV) error {
	thresholdCfg := cfg.Rejection.Threshold
	if thresholdCfg.CPUUtilization > 1 || thresholdCfg.CPUUtilization < 0 {
		return errors.New("cpu_utilization must be between 0 and 1")
	}

	if thresholdCfg.CPUUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.CPU)) {
		return errors.New("monitored_resources config must include \"cpu\" as well")
	}

	if thresholdCfg.HeapUtilization > 1 || thresholdCfg.HeapUtilization < 0 {
		return errors.New("heap_utilization must be between 0 and 1")
	}

	if thresholdCfg.HeapUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.Heap)) {
		return errors.New("monitored_resources config must include \"heap\" as well")
	}

	return nil
}
