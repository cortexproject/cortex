package engine

import (
	"flag"
	"fmt"
	"strings"

	"github.com/thanos-io/promql-engine/logicalplan"
)

var supportedOptimizers = []string{"default", "all", "propagate-matchers", "sort-matchers", "merge-selects", "detect-histogram-stats"}

// ThanosEngineConfig contains the configuration to create engine
type ThanosEngineConfig struct {
	Enabled           bool                    `yaml:"enabled"`
	EnableXFunctions  bool                    `yaml:"enable_x_functions"`
	Optimizers        string                  `yaml:"optimizers"`
	LogicalOptimizers []logicalplan.Optimizer `yaml:"-"`
}

func (cfg *ThanosEngineConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"thanos-engine", false, "Experimental. Use Thanos promql engine https://github.com/thanos-io/promql-engine rather than the Prometheus promql engine.")
	f.BoolVar(&cfg.EnableXFunctions, prefix+"enable-x-functions", false, "Enable xincrease, xdelta, xrate etc from Thanos engine.")
	f.StringVar(&cfg.Optimizers, prefix+"optimizers", "default", "Logical plan optimizers. Multiple optimizers can be provided as a comma-separated list. Supported values: "+strings.Join(supportedOptimizers, ", "))
}

func (cfg *ThanosEngineConfig) Validate() error {
	splitOptimizers := strings.Split(cfg.Optimizers, ",")

	for _, optimizer := range splitOptimizers {
		if optimizer == "all" || optimizer == "default" {
			if len(splitOptimizers) > 1 {
				return fmt.Errorf("special optimizer %s cannot be combined with other optimizers", optimizer)
			}
		}
		optimizers, err := getOptimizer(optimizer)
		if err != nil {
			return err
		}
		cfg.LogicalOptimizers = append(cfg.LogicalOptimizers, optimizers...)
	}

	return nil
}

func getOptimizer(name string) ([]logicalplan.Optimizer, error) {
	switch name {
	case "default":
		return logicalplan.DefaultOptimizers, nil
	case "all":
		return logicalplan.AllOptimizers, nil
	case "propagate-matchers":
		return []logicalplan.Optimizer{logicalplan.PropagateMatchersOptimizer{}}, nil
	case "sort-matchers":
		return []logicalplan.Optimizer{logicalplan.SortMatchers{}}, nil
	case "merge-selects":
		return []logicalplan.Optimizer{logicalplan.MergeSelectsOptimizer{}}, nil
	case "detect-histogram-stats":
		return []logicalplan.Optimizer{logicalplan.DetectHistogramStatsOptimizer{}}, nil
	default:
		return nil, fmt.Errorf("unknown optimizer %s", name)
	}
}
