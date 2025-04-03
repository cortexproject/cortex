package configs

import (
	"errors"
	"flag"
)

var ErrInvalidResourceThreshold = errors.New("invalid resource utilization threshold, it must be between 0 and 1")

type Resources struct {
	CPU  float64 `yaml:"cpu"`
	Heap float64 `yaml:"heap"`
}

func (cfg *Resources) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&cfg.CPU, "resource-thresholds.cpu", 0, "Utilization threshold for CPU in percentage, between 0 and 1. 0 to disable. The CPU utilization metric is from github.com/prometheus/procfs, which is a close estimate. Applicable to ingesters and store-gateways only.")
	f.Float64Var(&cfg.Heap, "resource-thresholds.heap", 0, "Utilization threshold for heap in percentage, between 0 and 1. 0 to disable. The heap utilization metric is from runtime/metrics, which is a close estimate. Applicable to ingesters and store-gateways only.")
}

func (cfg *Resources) Validate() error {
	if cfg.CPU > 1 || cfg.CPU < 0 || cfg.Heap > 1 || cfg.Heap < 0 {
		return ErrInvalidResourceThreshold
	}

	return nil
}
