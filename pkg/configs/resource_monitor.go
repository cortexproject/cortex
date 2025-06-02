package configs

import (
	"flag"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/resource"
)

type ResourceMonitor struct {
	Resources       flagext.StringSliceCSV `yaml:"resources"`
	Interval        time.Duration          `yaml:"interval"`
	CPURateInterval time.Duration          `yaml:"cpu_rate_interval"`
}

func (cfg *ResourceMonitor) RegisterFlags(f *flag.FlagSet) {
	cfg.Resources = []string{}

	f.Var(&cfg.Resources, "resource-monitor.resources", "Comma-separated list of resources to monitor. "+
		"Supported values are cpu and heap, which tracks metrics from github.com/prometheus/procfs and runtime/metrics "+
		"that are close estimates. Empty string to disable.")
	f.DurationVar(&cfg.Interval, "resource-monitor.interval", 100*time.Millisecond, "Update interval of resource monitor. Must be greater than 0.")
	f.DurationVar(&cfg.CPURateInterval, "resource-monitor.cpu-rate-interval", time.Minute, "Interval to calculate average CPU rate. Must be greater than 0.")
}

func (cfg *ResourceMonitor) Validate() error {
	for _, r := range cfg.Resources {
		switch resource.Type(r) {
		case resource.CPU, resource.Heap:
		default:
			if len(r) > 0 {
				return fmt.Errorf("unsupported resource type to monitor: %s", r)
			}
		}
	}

	if cfg.Interval <= 0 {
		return fmt.Errorf("resource monitor interval must be greater than zero")
	}

	if cfg.CPURateInterval <= 0 {
		return fmt.Errorf("resource monitor cpu rate interval must be greater than zero")
	}

	return nil
}
