package client

import (
	"context"
	"flag"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// Config says where we can find the ruler configs.
type Config struct {
	DBConfig db.Config

	// DEPRECATED
	ConfigsAPIURL flagext.URLValue

	// DEPRECATED. HTTP timeout duration for requests made to the Weave Cloud
	// configs service.
	ClientTimeout time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.DBConfig.RegisterFlags(f)
	flag.Var(&cfg.ConfigsAPIURL, "alertmanager.configs.url", "URL of configs API server.")
	flag.DurationVar(&cfg.ClientTimeout, "alertmanager.configs.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")
}

var configsRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "configs_request_duration_seconds",
	Help:      "Time spent requesting configs.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation", "status_code"}))

func init() {
	configsRequestDuration.Register()
}

type instrumented struct {
	next Client
}

func (i instrumented) GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error) {
	var cfgs *ConfigsResponse
	err := instrument.CollectedRequest(context.Background(), "Configs.GetConfigs", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		cfgs, err = i.next.GetAlerts(ctx, since) // Warning: this will produce an incorrect result if the configID ever overflows
		return err
	})
	return cfgs, err
}
