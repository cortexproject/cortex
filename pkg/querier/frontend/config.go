package frontend

import (
	"flag"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/frontend2"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// This struct combines several configuration options together to preserve backwards compatibility.
type CombinedFrontendConfig struct {
	Handler   HandlerConfig    `yaml:",inline"`
	Frontend  Config           `yaml:",inline"`
	Frontend2 frontend2.Config `yaml:",inline"`

	CompressResponses bool   `yaml:"compress_responses"`
	DownstreamURL     string `yaml:"downstream_url"`
}

func (cfg *CombinedFrontendConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Handler.RegisterFlags(f)
	cfg.Frontend.RegisterFlags(f)
	cfg.Frontend2.RegisterFlags(f)

	f.BoolVar(&cfg.CompressResponses, "querier.compress-http-responses", false, "Compress HTTP responses.")
	f.StringVar(&cfg.DownstreamURL, "frontend.downstream-url", "", "URL of downstream Prometheus.")
}

func (cfg *CombinedFrontendConfig) InitFrontend(limits Limits, grpcListenPort int, log log.Logger, reg prometheus.Registerer) (http.RoundTripper, *Frontend, *frontend2.Frontend2, error) {
	switch {
	case cfg.DownstreamURL != "":
		// If the user has specified a downstream Prometheus, then we should use that.
		rt, err := NewDownstreamRoundTripper(cfg.DownstreamURL)
		return rt, nil, nil, err

	case cfg.Frontend2.SchedulerAddr != "":
		// If query-scheduler address is configured, use Frontend2.
		if cfg.Frontend2.Addr == "" {
			addr, err := util.GetFirstAddressOf(cfg.Frontend2.InfNames)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to get frontend address")
			}

			cfg.Frontend2.Addr = addr
		}

		if cfg.Frontend2.Port == 0 {
			cfg.Frontend2.Port = grpcListenPort
		}

		fr, err := frontend2.NewFrontend2(cfg.Frontend2, log, reg)
		return AdaptGrpcRoundTripperToHTTPRoundTripper(fr), nil, fr, err

	default:
		// No scheduler = use original frontend.
		fr, err := New(cfg.Frontend, limits, log, reg)
		if err != nil {
			return nil, nil, nil, err
		}

		return AdaptGrpcRoundTripperToHTTPRoundTripper(fr), fr, nil, err
	}
}

// Configuration for both querier workers, V1 (using frontend) and V2 (using scheduler). Since many flags are reused
// between the two, they are exposed to YAML/CLI in V1 version (WorkerConfig), and copied to V2 in the init method.
type CombinedWorkerConfig struct {
	WorkerV1 WorkerConfig                   `yaml:",inline"`
	WorkerV2 frontend2.QuerierWorkersConfig `yaml:",inline"`
}

func (cfg *CombinedWorkerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.WorkerV1.RegisterFlags(f)
	cfg.WorkerV2.RegisterFlags(f)
}

func (cfg *CombinedWorkerConfig) InitQuerierWorker(querierCfg querier.Config, handler http.Handler, log log.Logger) (services.Service, error) {
	switch {
	case cfg.WorkerV2.SchedulerAddr != "":
		// Copy settings from querier v1 config struct.
		cfg.WorkerV2.GRPCClientConfig = cfg.WorkerV1.GRPCClientConfig
		cfg.WorkerV2.MatchMaxConcurrency = cfg.WorkerV1.MatchMaxConcurrency
		cfg.WorkerV2.MaxConcurrentRequests = querierCfg.MaxConcurrent
		cfg.WorkerV2.Parallelism = cfg.WorkerV1.Parallelism
		cfg.WorkerV2.QuerierID = cfg.WorkerV1.QuerierID

		level.Info(log).Log("msg", "Starting querier worker v2 with scheduler", "scheduler", cfg.WorkerV2.SchedulerAddr)
		return frontend2.NewQuerierSchedulerWorkers(cfg.WorkerV2, httpgrpc_server.NewServer(handler), prometheus.DefaultRegisterer, log)

	case cfg.WorkerV1.FrontendAddress != "":
		level.Info(log).Log("msg", "Starting querier worker v1 with frontend", "frontend", cfg.WorkerV1.FrontendAddress)
		return NewWorker(cfg.WorkerV1, querierCfg, httpgrpc_server.NewServer(handler), log)

	default:
		// No querier worker is necessary, querier will receive queries directly from HTTP server.
		return nil, nil
	}
}

func (cfg *CombinedWorkerConfig) Validate(logger log.Logger) error {
	return cfg.WorkerV1.Validate(logger)
}
