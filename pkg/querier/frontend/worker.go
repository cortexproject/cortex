package frontend

import (
	"context"
	"flag"
	"math/rand"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// WorkerConfig is config for a worker.
type WorkerConfig struct {
	Address             string        `yaml:"frontend_address"`
	Parallelism         int           `yaml:"parallelism"`
	MatchMaxConcurrency bool          `yaml:"match_max_concurrent"`
	DNSLookupDuration   time.Duration `yaml:"dns_lookup_duration"`

	GRPCClientConfig grpcclient.ConfigWithTLS `yaml:"grpc_client_config"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *WorkerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "querier.frontend-address", "", "Address of query frontend service, in host:port format.")
	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 10, "Number of simultaneous queries to process per query frontend.")
	f.BoolVar(&cfg.MatchMaxConcurrency, "querier.worker-match-max-concurrent", false, "Force worker concurrency to match the -querier.max-concurrent option.  Overrides querier.worker-parallelism.")
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", f)

	// TODO: Remove this flag in v1.6.0.
	f.DurationVar(&cfg.DNSLookupDuration, "querier.dns-lookup-period", 0, "[DEPRECATED] How often to query DNS. This flag is ignored.")
}

// Worker is the counter-part to the frontend, actually processing requests.
type worker struct {
	cfg        WorkerConfig
	querierCfg querier.Config
	log        log.Logger
	server     *server.Server
	service    *services.BasicService

	managers map[string]*frontendManager
}

// NewWorker creates a new worker and returns a service that is wrapping it.
// If no address is specified, it returns nil service (and no error).
func NewWorker(cfg WorkerConfig, querierCfg querier.Config, server *server.Server, log log.Logger) (services.Service, error) {
	if cfg.Address == "" {
		level.Info(log).Log("msg", "no address specified, not starting worker")
		return nil, nil
	}

	if cfg.DNSLookupDuration != 0 {
		flagext.DeprecatedFlagsUsed.Inc()
		level.Warn(log).Log("msg", "Using deprecated flag -querier.dns-lookup-period, this flag is ignored")
	}

	w := &worker{
		cfg:        cfg,
		querierCfg: querierCfg,
		log:        log,
		server:     server,
		managers:   map[string]*frontendManager{},
	}
	w.service = services.NewBasicService(nil, w.running, w.stopping)

	return w.service, nil
}

func (w *worker) stopping(_ error) error {
	// wait until all per-address workers are done. This is only called after watchDNSLoop exits.
	for _, mgr := range w.managers {
		mgr.stop()
	}
	return nil
}

// UpdateState implements resolver.ClientConn interface.
// It adds or removes workers based on updated DNS state.
func (w *worker) UpdateState(state resolver.State) {
	newManagers := make(map[string]*frontendManager, len(state.Addresses))

	// Add new addresses.
	for _, addr := range state.Addresses {
		if m, ok := w.managers[addr.Addr]; ok {
			newManagers[addr.Addr] = m
			continue
		}

		level.Debug(w.log).Log("msg", "adding connection", "addr", addr.Addr)
		client, err := w.connect(w.service.ServiceContext(), addr.Addr)
		if err != nil {
			level.Error(w.log).Log("msg", "error connecting", "addr", addr.Addr, "err", err)
			continue
		}
		newManagers[addr.Addr] = newFrontendManager(w.service.ServiceContext(), w.log, w.server, client, w.cfg.GRPCClientConfig)
	}

	// Stop old addresses.
	for addr, mngr := range w.managers {
		if _, ok := newManagers[addr]; ok {
			continue
		}
		level.Debug(w.log).Log("msg", "removing connection", "addr", addr)
		mngr.stop()
	}

	// TODO: Check if UpdateState can be called concurrently.
	w.managers = newManagers
	w.resetConcurrency()
}

// ReportError implements resolver.ClientConn interface.
func (w *worker) ReportError(err error) {
	// TODO: How to exit on non-recoverable error. 'naming' package had it.
	level.Error(w.log).Log("msg", "resolver error", "err", err)
}

// NewAddress implements resolver.ClientConn interface.
// Deprecated in favour of UpdateState.
func (w *worker) NewAddress([]resolver.Address) {}

// NewServiceConfig implements resolver.ClientConn interface.
// Deprecated in favour of UpdateState.
func (w *worker) NewServiceConfig(string) {}

// ParseServiceConfig implements resolver.ClientConn interface.
func (w *worker) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return nil
}

// watchDNSLoop watches for changes in DNS and starts or stops workers.
func (w *worker) running(servCtx context.Context) error {
	builder := resolver.Get("dns")
	r, err := builder.Build(
		resolver.Target{
			Scheme:    "dns",
			Authority: "",
			Endpoint:  w.cfg.Address,
		},
		w,
		resolver.BuildOptions{},
	)
	if err != nil {
		return err
	}
	defer r.Close()

	<-servCtx.Done()
	return nil
}

func (w *worker) connect(ctx context.Context, address string) (FrontendClient, error) {
	opts, err := w.cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{middleware.ClientUserHeaderInterceptor}, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return NewFrontendClient(conn), nil
}

func (w *worker) resetConcurrency() {
	addresses := make([]string, 0, len(w.managers))
	for addr := range w.managers {
		addresses = append(addresses, addr)
	}
	rand.Shuffle(len(addresses), func(i, j int) { addresses[i], addresses[j] = addresses[j], addresses[i] })

	totalConcurrency := 0
	for i, addr := range addresses {
		concurrentRequests := w.concurrency(i, addr)
		totalConcurrency += concurrentRequests

		if mgr, ok := w.managers[addr]; ok {
			mgr.concurrentRequests(concurrentRequests)
		} else {
			level.Error(w.log).Log("msg", "address not found in managers map.  this should not happen", "addr", addr)
		}
	}

	if totalConcurrency > w.querierCfg.MaxConcurrent {
		level.Warn(w.log).Log("msg", "total worker concurrency is greater than promql max concurrency. queries may be queued in the querier which reduces QOS")
	}
}

func (w *worker) concurrency(index int, addr string) int {
	concurrentRequests := 0

	if w.cfg.MatchMaxConcurrency {
		concurrentRequests = w.querierCfg.MaxConcurrent / len(w.managers)

		// If max concurrency does not evenly divide into our frontends a subset will be chosen
		// to receive an extra connection.  Frontend addresses were shuffled above so this will be a
		// random selection of frontends.
		if index < w.querierCfg.MaxConcurrent%len(w.managers) {
			level.Warn(w.log).Log("msg", "max concurrency is not evenly divisible across query frontends. adding an extra connection", "addr", addr)
			concurrentRequests++
		}
	} else {
		concurrentRequests = w.cfg.Parallelism
	}

	// If concurrentRequests is 0 then w.querierCfg.MaxConcurrent is less than the total number of
	// query frontends. In order to prevent accidentally starving a frontend we are just going to
	// always connect once to every frontend.  This is dangerous b/c we may start exceeding promql
	// max concurrency.
	if concurrentRequests == 0 {
		concurrentRequests = 1
	}

	return concurrentRequests
}
