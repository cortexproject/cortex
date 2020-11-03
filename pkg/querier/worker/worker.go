package worker

import (
	"context"
	"flag"
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type Config struct {
	FrontendAddress  string        `yaml:"frontend_address"`
	SchedulerAddress string        `yaml:"scheduler_address"`
	DNSLookupPeriod  time.Duration `yaml:"dns_lookup_duration"`

	Parallelism           int  `yaml:"parallelism"`
	MatchMaxConcurrency   bool `yaml:"match_max_concurrent"`
	MaxConcurrentRequests int  `yaml:"-"` // Must be same as passed to PromQL Engine.

	QuerierID string `yaml:"id"`

	GRPCClientConfig grpcclient.ConfigWithTLS `yaml:"grpc_client_config"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "querier.scheduler-address", "", "Hostname (and port) of scheduler that querier will periodically resolve, connect to and receive queries from. If set, takes precedence over -querier.frontend-address.")
	f.StringVar(&cfg.FrontendAddress, "querier.frontend-address", "", "Address of query frontend service, in host:port format. If -querier.scheduler-address is set as well, querier will use scheduler instead. If neither -querier.frontend-address or -querier.scheduler-address is set, queries must arrive via HTTP endpoint.")

	f.DurationVar(&cfg.DNSLookupPeriod, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS for query-frontend or query-scheduler address.")

	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 10, "Number of simultaneous queries to process per query-frontend or query-scheduler.")
	f.BoolVar(&cfg.MatchMaxConcurrency, "querier.worker-match-max-concurrent", false, "Force worker concurrency to match the -querier.max-concurrent option. Overrides querier.worker-parallelism.")
	f.StringVar(&cfg.QuerierID, "querier.id", "", "Querier ID, sent to frontend service to identify requests from the same querier. Defaults to hostname.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", f)
}

func (cfg *Config) Validate(log log.Logger) error {
	return cfg.GRPCClientConfig.Validate(log)
}

// Handler for HTTP requests wrapped in protobuf messages.
type RequestHandler interface {
	Handle(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

// Single processor handles all streaming operations to query-frontend or query-scheduler to fetch queries
// and process them.
type processor interface {
	// Each invocation of processQueriesOnSingleStream starts new streaming operation to query-frontend
	// or query-scheduler to fetch queries and execute them.
	//
	// This method must react on context being finished, and stop when that happens.
	//
	// processorManager (not processor) is responsible for starting as many goroutines as needed for each connection.
	processQueriesOnSingleStream(ctx context.Context, conn *grpc.ClientConn, address string)
}

type querierWorker struct {
	*services.BasicService

	cfg Config
	log log.Logger

	processor processor

	subservices *services.Manager

	mu sync.Mutex
	// Set to nil when stop is called... no more managers are created afterwards.
	managers map[string]*processorManager
}

func NewQuerierWorker(cfg Config, handler RequestHandler, log log.Logger, reg prometheus.Registerer) (services.Service, error) {
	if cfg.QuerierID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get hostname for configuring querier ID")
		}
		cfg.QuerierID = hostname
	}

	var processor processor
	var servs []services.Service
	var address string

	switch {
	case cfg.SchedulerAddress != "":
		level.Info(log).Log("msg", "Starting querier worker connected to query-scheduler", "scheduler", cfg.SchedulerAddress)

		address = cfg.SchedulerAddress
		processor, servs = newSchedulerProcessor(cfg, handler, log, reg)

	case cfg.FrontendAddress != "":
		level.Info(log).Log("msg", "Starting querier worker connected to query-frontend", "frontend", cfg.FrontendAddress)

		address = cfg.FrontendAddress
		processor = newFrontendProcessor(cfg, handler, log)

	default:
		return nil, errors.New("no query-scheduler or query-frontend address")
	}

	return newQuerierWorkerWithProcessor(cfg, log, processor, address, servs)
}

func newQuerierWorkerWithProcessor(cfg Config, log log.Logger, processor processor, address string, servs []services.Service) (*querierWorker, error) {
	f := &querierWorker{
		cfg:       cfg,
		log:       log,
		managers:  map[string]*processorManager{},
		processor: processor,
	}

	w, err := util.NewDNSWatcher(address, cfg.DNSLookupPeriod, f)
	if err != nil {
		return nil, err
	}

	servs = append(servs, w)

	f.subservices, err = services.NewManager(servs...)
	if err != nil {
		return nil, errors.Wrap(err, "querier worker subservices")
	}

	f.BasicService = services.NewIdleService(f.starting, f.stopping)
	return f, nil
}

func (f *querierWorker) starting(ctx context.Context) error {
	return services.StartManagerAndAwaitHealthy(ctx, f.subservices)
}

func (f *querierWorker) stopping(_ error) error {
	err := services.StopManagerAndAwaitStopped(context.Background(), f.subservices)

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.managers {
		m.stop()
	}

	return err
}

func (f *querierWorker) AddressAdded(address string) {
	ctx := f.ServiceContext()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// We already have worker for this scheduler.
	if w := f.managers[address]; w != nil {
		return
	}

	level.Debug(f.log).Log("msg", "adding connection", "addr", address)
	conn, err := f.connect(context.Background(), address)
	if err != nil {
		level.Error(f.log).Log("msg", "error connecting", "addr", address, "err", err)
		return
	}

	// If not, start a new one.
	f.managers[address] = newProcessorManager(ctx, f.processor, conn, address)
	f.resetConcurrency() // Called with lock.
}

func (f *querierWorker) AddressRemoved(address string) {
	level.Debug(f.log).Log("msg", "removing connection", "addr", address)

	f.mu.Lock()
	p := f.managers[address]
	delete(f.managers, address)
	f.mu.Unlock()

	if p != nil {
		p.stop()
	}
}

// Must be called with lock.
func (f *querierWorker) resetConcurrency() {
	totalConcurrency := 0
	index := 0

	for _, w := range f.managers {
		concurrency := 0

		if f.cfg.MatchMaxConcurrency {
			concurrency = f.cfg.MaxConcurrentRequests / len(f.managers)

			// If max concurrency does not evenly divide into our frontends a subset will be chosen
			// to receive an extra connection.  Frontend addresses were shuffled above so this will be a
			// random selection of frontends.
			if index < f.cfg.MaxConcurrentRequests%len(f.managers) {
				concurrency++
			}
		} else {
			concurrency = f.cfg.Parallelism
		}

		// If concurrency is 0 then MaxConcurrentRequests is less than the total number of
		// frontends/schedulers. In order to prevent accidentally starving a frontend or scheduler we are just going to
		// always connect once to every target.  This is dangerous b/c we may start exceeding PromQL
		// max concurrency.
		if concurrency == 0 {
			concurrency = 1
		}

		totalConcurrency += concurrency
		w.concurrency(concurrency)
		index++
	}

	if totalConcurrency > f.cfg.MaxConcurrentRequests {
		level.Warn(f.log).Log("msg", "total worker concurrency is greater than promql max concurrency. queries may be queued in the querier which reduces QOS")
	}
}

func (f *querierWorker) connect(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := f.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
