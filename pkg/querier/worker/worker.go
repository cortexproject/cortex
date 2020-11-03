package worker

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"

	"github.com/cortexproject/cortex/pkg/frontend/v1/frontendv1pb"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// WorkerConfig is config for a worker.
// nolint:golint
type WorkerConfig struct {
	FrontendAddress     string        `yaml:"frontend_address"`
	Parallelism         int           `yaml:"parallelism"`
	MatchMaxConcurrency bool          `yaml:"match_max_concurrent"`
	DNSLookupDuration   time.Duration `yaml:"dns_lookup_duration"`
	QuerierID           string        `yaml:"id"`

	GRPCClientConfig grpcclient.ConfigWithTLS `yaml:"grpc_client_config"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *WorkerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.FrontendAddress, "querier.frontend-address", "", "Address of query frontend service, in host:port format. If -querier.scheduler-address is set as well, querier will use scheduler instead. If neither -querier.frontend-address or -querier.scheduler-address is set, queries must arrive via HTTP endpoint.")
	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 10, "Number of simultaneous queries to process per query frontend.")
	f.BoolVar(&cfg.MatchMaxConcurrency, "querier.worker-match-max-concurrent", false, "Force worker concurrency to match the -querier.max-concurrent option.  Overrides querier.worker-parallelism.")
	f.DurationVar(&cfg.DNSLookupDuration, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS.")
	f.StringVar(&cfg.QuerierID, "querier.id", "", "Querier ID, sent to frontend service to identify requests from the same querier. Defaults to hostname.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", f)
}

func (cfg *WorkerConfig) Validate(log log.Logger) error {
	return cfg.GRPCClientConfig.Validate(log)
}

// Worker is the counter-part to the frontend, actually processing requests.
type worker struct {
	cfg        WorkerConfig
	querierCfg querier.Config
	log        log.Logger
	server     *server.Server

	watcher  naming.Watcher //nolint:staticcheck //Skipping for now. If you still see this more than likely issue https://github.com/cortexproject/cortex/issues/2015 has not yet been addressed.
	managers map[string]*frontendManager
}

// NewWorker creates a new worker and returns a service that is wrapping it.
// If no address is specified, it returns error.
func NewWorker(cfg WorkerConfig, querierCfg querier.Config, server *server.Server, log log.Logger) (services.Service, error) {
	if cfg.FrontendAddress == "" {
		return nil, errors.New("frontend address not configured")
	}

	if cfg.QuerierID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, errors.Wrap(err, "unable to get hostname used to initialise default querier ID")
		}
		cfg.QuerierID = hostname
	}

	resolver, err := naming.NewDNSResolverWithFreq(cfg.DNSLookupDuration)
	if err != nil {
		return nil, err
	}

	watcher, err := resolver.Resolve(cfg.FrontendAddress)
	if err != nil {
		return nil, err
	}

	w := &worker{
		cfg:        cfg,
		querierCfg: querierCfg,
		log:        log,
		server:     server,
		watcher:    watcher,
		managers:   map[string]*frontendManager{},
	}
	return services.NewBasicService(nil, w.watchDNSLoop, w.stopping), nil
}

func (w *worker) stopping(_ error) error {
	// wait until all per-address workers are done. This is only called after watchDNSLoop exits.
	for _, mgr := range w.managers {
		mgr.stop()
	}
	return nil
}

// watchDNSLoop watches for changes in DNS and starts or stops workers.
func (w *worker) watchDNSLoop(servCtx context.Context) error {
	go func() {
		// Close the watcher, when this service is asked to stop.
		// Closing the watcher makes watchDNSLoop exit, since it only iterates on watcher updates, and has no other
		// way to stop. We cannot close the watcher in `stopping` method, because it is only called *after*
		// watchDNSLoop exits.
		<-servCtx.Done()
		w.watcher.Close()
	}()

	for {
		updates, err := w.watcher.Next()
		if err != nil {
			// watcher.Next returns error when Close is called, but we call Close when our context is done.
			// we don't want to report error in that case.
			if servCtx.Err() != nil {
				return nil
			}
			return errors.Wrapf(err, "error from DNS watcher")
		}

		for _, update := range updates {
			switch update.Op {
			case naming.Add:
				level.Debug(w.log).Log("msg", "adding connection", "addr", update.Addr)
				conn, err := w.connect(servCtx, update.Addr)
				if err != nil {
					level.Error(w.log).Log("msg", "error connecting", "addr", update.Addr, "err", err)
					continue
				}

				w.managers[update.Addr] = newFrontendManager(servCtx, w.log, w.server, conn, frontendv1pb.NewFrontendClient(conn), w.cfg.GRPCClientConfig, w.cfg.QuerierID)

			case naming.Delete:
				level.Debug(w.log).Log("msg", "removing connection", "addr", update.Addr)
				if mgr, ok := w.managers[update.Addr]; ok {
					mgr.stop()
					delete(w.managers, update.Addr)
				}

			default:
				return fmt.Errorf("unknown op: %v", update.Op)
			}
		}

		w.resetConcurrency()
	}
}

func (w *worker) connect(ctx context.Context, address string) (*grpc.ClientConn, error) {
	opts, err := w.cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{middleware.ClientUserHeaderInterceptor}, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
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
