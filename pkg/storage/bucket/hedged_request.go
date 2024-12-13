package bucket

import (
	"errors"
	"flag"
	"net/http"

	"github.com/thanos-io/thanos/pkg/exthttp"
)

var (
	errInvalidQuantile = errors.New("invalid hedged request quantile, it must be between 0 and 1")
)

type HedgedRequestConfig struct {
	Enabled     bool    `yaml:"enabled"`
	MaxRequests uint    `yaml:"max_requests"`
	Quantile    float64 `yaml:"quantile"`
}

func (cfg *HedgedRequestConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"hedged-request.enabled", false, "If true, hedged requests are applied to object store calls. It can help with reducing tail latency.")
	f.UintVar(&cfg.MaxRequests, prefix+"hedged-request.max-requests", 3, "Maximum number of hedged requests allowed for each initial request. A high number can reduce latency but increase internal calls.")
	f.Float64Var(&cfg.Quantile, prefix+"hedged-request.quantile", 0.9, "It is used to calculate a latency threshold to trigger hedged requests. For example, additional requests are triggered when the initial request response time exceeds the 90th percentile.")
}

func (cfg *HedgedRequestConfig) GetHedgedRoundTripper() func(rt http.RoundTripper) http.RoundTripper {
	return exthttp.CreateHedgedTransportWithConfig(exthttp.CustomBucketConfig{
		HedgingConfig: exthttp.HedgingConfig{
			Enabled:  cfg.Enabled,
			UpTo:     cfg.MaxRequests,
			Quantile: cfg.Quantile,
		},
	})
}

func (cfg *HedgedRequestConfig) Validate() error {
	if cfg.Quantile > 1 || cfg.Quantile < 0 {
		return errInvalidQuantile
	}

	return nil
}
