package engine

import (
	"flag"
	"fmt"
	"strings"

	"github.com/thanos-io/promql-engine/logicalplan"
)

var supportedOptimizers = []string{"default", "all", "propagate-matchers", "sort-matchers", "merge-selects", "detect-histogram-stats"}

// Config contains the configuration to create engine
type Config struct {
	EnableThanosEngine bool                    `yaml:"enable_thanos_engine"`
	EnableXFunctions   bool                    `yaml:"enable_x_functions"`
	Optimizers         string                  `yaml:"optimizers"`
	LogicalOptimizers  []logicalplan.Optimizer `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnableThanosEngine, "querier.thanos-engine", false, "Experimental. Use Thanos promql engine https://github.com/thanos-io/promql-engine rather than the Prometheus promql engine.")
	f.BoolVar(&cfg.EnableXFunctions, "querier.enable-x-functions", false, "Enable xincrease, xdelta, xrate etc from Thanos engine.")
	f.StringVar(&cfg.Optimizers, "querier.optimizers", "default", "Logical plan optimizers. Multiple optimizers can be provided as a comma-separated list. Supported values: "+strings.Join(supportedOptimizers, ", "))
}

func (cfg *Config) Validate() error {
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
