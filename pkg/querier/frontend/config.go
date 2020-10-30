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
	Handler    HandlerConfig    `yaml:",inline"`
	FrontendV1 Config           `yaml:",inline"`
	FrontendV2 frontend2.Config `yaml:",inline"`

	CompressResponses bool   `yaml:"compress_responses"`
	DownstreamURL     string `yaml:"downstream_url"`
}

func (cfg *CombinedFrontendConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Handler.RegisterFlags(f)
	cfg.FrontendV1.RegisterFlags(f)
	cfg.FrontendV2.RegisterFlags(f)

	f.BoolVar(&cfg.CompressResponses, "querier.compress-http-responses", false, "Compress HTTP responses.")
	f.StringVar(&cfg.DownstreamURL, "frontend.downstream-url", "", "URL of downstream Prometheus.")
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

func (cfg *CombinedWorkerConfig) Validate(logger log.Logger) error {
	return cfg.WorkerV1.Validate(logger)
}

// Initializes frontend (either V1 -- without scheduler, or V2 -- with scheduler) or no frontend at
// all if downstream Prometheus URL is used instead.
//
// Returned RoundTripper can be wrapped in more round-tripper middlewares, and then eventually registered
// into HTTP server using the Handler from this package. Returned RoundTripper is always non-nil
// (if there are no errors), and it uses the returned frontend (if any).
func InitFrontend(cfg CombinedFrontendConfig, limits Limits, grpcListenPort int, log log.Logger, reg prometheus.Registerer) (http.RoundTripper, *Frontend, *frontend2.Frontend2, error) {
	switch {
	case cfg.DownstreamURL != "":
		// If the user has specified a downstream Prometheus, then we should use that.
		rt, err := NewDownstreamRoundTripper(cfg.DownstreamURL)
		return rt, nil, nil, err

	case cfg.FrontendV2.SchedulerAddress != "":
		// If query-scheduler address is configured, use Frontend2.
		if cfg.FrontendV2.Addr == "" {
			addr, err := util.GetFirstAddressOf(cfg.FrontendV2.InfNames)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to get frontend address")
			}

			cfg.FrontendV2.Addr = addr
		}

		if cfg.FrontendV2.Port == 0 {
			cfg.FrontendV2.Port = grpcListenPort
		}

		fr, err := frontend2.NewFrontend2(cfg.FrontendV2, log, reg)
		return AdaptGrpcRoundTripperToHTTPRoundTripper(fr), nil, fr, err

	default:
		// No scheduler = use original frontend.
		fr, err := New(cfg.FrontendV1, limits, log, reg)
		if err != nil {
			return nil, nil, nil, err
		}

		return AdaptGrpcRoundTripperToHTTPRoundTripper(fr), fr, nil, err
	}
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
		return frontend2.NewQuerierSchedulerWorkers(cfg.WorkerV2, httpgrpc_server.NewServer(handler), prometheus.DefaultRegisterer, log)

	case cfg.WorkerV1.FrontendAddress != "":
		level.Info(log).Log("msg", "Starting querier worker connected to query-frontend", "frontend", cfg.WorkerV1.FrontendAddress)
		return NewWorker(cfg.WorkerV1, querierCfg, httpgrpc_server.NewServer(handler), log)

	default:
		return nil, nil
	}
}
