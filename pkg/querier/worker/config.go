package worker

import (
	"flag"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc/server"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Configuration for both querier workers, V1 (using frontend) and V2 (using scheduler). Since many flags are reused
// between the two, they are exposed to YAML/CLI in V1 version (WorkerConfig), and copied to V2 in the init method.
type CombinedWorkerConfig struct {
	WorkerV1 WorkerConfig         `yaml:",inline"`
	WorkerV2 QuerierWorkersConfig `yaml:",inline"`
}

func (cfg *CombinedWorkerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.WorkerV1.RegisterFlags(f)
	cfg.WorkerV2.RegisterFlags(f)
}

func (cfg *CombinedWorkerConfig) Validate(logger log.Logger) error {
	return cfg.WorkerV1.Validate(logger)
}

// Initializes querier-worker, which uses either configured query-scheduler or query-frontend,
// or if none is specified and no worker is necessary returns nil (in that case queries are
// received directly from HTTP server).
func InitQuerierWorker(cfg CombinedWorkerConfig, querierCfg querier.Config, handler http.Handler, log log.Logger) (services.Service, error) {
	switch {
	case cfg.WorkerV2.SchedulerAddress != "":
		// Copy settings from querier v1 config struct.
		cfg.WorkerV2.GRPCClientConfig = cfg.WorkerV1.GRPCClientConfig
		cfg.WorkerV2.MatchMaxConcurrency = cfg.WorkerV1.MatchMaxConcurrency
		cfg.WorkerV2.MaxConcurrentRequests = querierCfg.MaxConcurrent
		cfg.WorkerV2.Parallelism = cfg.WorkerV1.Parallelism
		cfg.WorkerV2.QuerierID = cfg.WorkerV1.QuerierID

		level.Info(log).Log("msg", "Starting querier worker connected to query-scheduler", "scheduler", cfg.WorkerV2.SchedulerAddress)
		return NewQuerierSchedulerWorkers(cfg.WorkerV2, server.NewServer(handler), prometheus.DefaultRegisterer, log)

	case cfg.WorkerV1.FrontendAddress != "":
		level.Info(log).Log("msg", "Starting querier worker connected to query-frontend", "frontend", cfg.WorkerV1.FrontendAddress)
		return NewWorker(cfg.WorkerV1, querierCfg, server.NewServer(handler), log)

	default:
		return nil, nil
	}
}
