package configs

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/resource"
)

// recognizedEvictionMetrics lists the valid values for eviction_metric.
var recognizedEvictionMetrics = map[string]bool{
	"fetched_samples":     true,
	"fetched_series":      true,
	"fetched_chunks":      true,
	"fetched_chunk_bytes": true,
}

type QueryProtection struct {
	Rejection rejection      `json:"rejection"`
	Eviction  EvictionConfig `yaml:"eviction"`
}

type rejection struct {
	Threshold Threshold `yaml:"threshold"`
}

// Threshold holds CPU and heap utilization thresholds (0-1 range).
type Threshold struct {
	CPUUtilization  float64 `yaml:"cpu_utilization"`
	HeapUtilization float64 `yaml:"heap_utilization"`
}

// EvictionConfig configures the resource-based query evictor.
type EvictionConfig struct {
	Threshold            Threshold     `yaml:"threshold"`
	CheckInterval        time.Duration `yaml:"check_interval"`
	CooldownPeriod       int           `yaml:"cooldown_period"`
	EvictionMetric       string        `yaml:"eviction_metric"`
	MinQueryAge          time.Duration `yaml:"min_query_age"`
	MaxEvictionsPerCycle int           `yaml:"max_evictions_per_cycle"`
}

// Enabled returns true when at least one eviction threshold is greater than 0.
func (c EvictionConfig) Enabled() bool {
	return c.Threshold.CPUUtilization > 0 || c.Threshold.HeapUtilization > 0
}

func (cfg *QueryProtection) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	// Rejection flags
	f.Float64Var(&cfg.Rejection.Threshold.CPUUtilization, prefix+"query-protection.rejection.threshold.cpu-utilization", 0, "EXPERIMENTAL: Max CPU utilization that this instance can reach before rejecting new query request (across all tenants) in percentage, between 0 and 1. monitored_resources config must include the resource type. 0 to disable.")
	f.Float64Var(&cfg.Rejection.Threshold.HeapUtilization, prefix+"query-protection.rejection.threshold.heap-utilization", 0, "EXPERIMENTAL: Max heap utilization that this instance can reach before rejecting new query request (across all tenants) in percentage, between 0 and 1. monitored_resources config must include the resource type. 0 to disable.")

	// Eviction flags
	f.Float64Var(&cfg.Eviction.Threshold.CPUUtilization, prefix+"query-protection.eviction.threshold.cpu-utilization", 0, "EXPERIMENTAL: Max CPU utilization that this instance can reach before evicting the heaviest running query (across all tenants) in percentage, between 0 and 1. monitored_resources config must include the resource type. 0 to disable.")
	f.Float64Var(&cfg.Eviction.Threshold.HeapUtilization, prefix+"query-protection.eviction.threshold.heap-utilization", 0, "EXPERIMENTAL: Max heap utilization that this instance can reach before evicting the heaviest running query (across all tenants) in percentage, between 0 and 1. monitored_resources config must include the resource type. 0 to disable.")
	f.DurationVar(&cfg.Eviction.CheckInterval, prefix+"query-protection.eviction.check-interval", 1*time.Second, "EXPERIMENTAL: How frequently the evictor checks system resource utilization.")
	f.IntVar(&cfg.Eviction.CooldownPeriod, prefix+"query-protection.eviction.cooldown-period", 3, "EXPERIMENTAL: Number of check intervals to wait after an eviction before evicting again.")
	f.StringVar(&cfg.Eviction.EvictionMetric, prefix+"query-protection.eviction.eviction-metric", "fetched_samples", "EXPERIMENTAL: The query metric used to determine the heaviest query for eviction. Supported values: fetched_samples, fetched_series, fetched_chunks, fetched_chunk_bytes.")
	f.DurationVar(&cfg.Eviction.MinQueryAge, prefix+"query-protection.eviction.min-query-age", 10*time.Second, "EXPERIMENTAL: Minimum time a query must be running before it becomes eligible for eviction. Queries younger than this are ignored.")
	f.IntVar(&cfg.Eviction.MaxEvictionsPerCycle, prefix+"query-protection.eviction.max-evictions-per-cycle", 1, "EXPERIMENTAL: Maximum number of queries to evict in a single check cycle when resource thresholds are breached.")
}

func (cfg *QueryProtection) Validate(monitoredResources flagext.StringSliceCSV) error {
	// Validate rejection thresholds
	rejThreshold := cfg.Rejection.Threshold
	if rejThreshold.CPUUtilization > 1 || rejThreshold.CPUUtilization < 0 {
		return errors.New("cpu_utilization must be between 0 and 1")
	}

	if rejThreshold.CPUUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.CPU)) {
		return errors.New("monitored_resources config must include \"cpu\" as well")
	}

	if rejThreshold.HeapUtilization > 1 || rejThreshold.HeapUtilization < 0 {
		return errors.New("heap_utilization must be between 0 and 1")
	}

	if rejThreshold.HeapUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.Heap)) {
		return errors.New("monitored_resources config must include \"heap\" as well")
	}

	// Validate eviction thresholds
	evThreshold := cfg.Eviction.Threshold
	if evThreshold.CPUUtilization > 1 || evThreshold.CPUUtilization < 0 {
		return errors.New("eviction cpu_utilization must be between 0 and 1")
	}

	if evThreshold.HeapUtilization > 1 || evThreshold.HeapUtilization < 0 {
		return errors.New("eviction heap_utilization must be between 0 and 1")
	}

	if cfg.Eviction.Enabled() {
		if cfg.Eviction.CheckInterval <= 0 {
			return errors.New("eviction check_interval must be greater than 0 when eviction is enabled")
		}

		if cfg.Eviction.CooldownPeriod < 0 {
			return errors.New("eviction cooldown_period must be >= 0")
		}

		if !recognizedEvictionMetrics[cfg.Eviction.EvictionMetric] {
			return fmt.Errorf("unrecognized eviction_metric %q; supported values: fetched_samples, fetched_series, fetched_chunks, fetched_chunk_bytes", cfg.Eviction.EvictionMetric)
		}

		if cfg.Eviction.MaxEvictionsPerCycle < 1 {
			return errors.New("eviction max_evictions_per_cycle must be >= 1")
		}

		if evThreshold.CPUUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.CPU)) {
			return errors.New("monitored_resources config must include \"cpu\" when eviction cpu threshold is set")
		}

		if evThreshold.HeapUtilization > 0 && !strings.Contains(monitoredResources.String(), string(resource.Heap)) {
			return errors.New("monitored_resources config must include \"heap\" when eviction heap threshold is set")
		}
	}

	return nil
}
