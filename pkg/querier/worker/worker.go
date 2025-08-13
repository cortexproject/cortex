package worker

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
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

	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`

	TargetHeaders []string `yaml:"-"` // Propagated by config.

	QuerierRing RingConfig `yaml:"ring"`
}

type RingConfig struct {
	KVStore                kv.Config     `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances."`
	HeartbeatPeriod        time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout       time.Duration `yaml:"heartbeat_timeout"`
	ReplicationFactor      int           `yaml:"replication_factor"`
	ZoneAwarenessEnabled   bool          `yaml:"zone_awareness_enabled"`
	TokensFilePath         string        `yaml:"tokens_file_path"`
	DetailedMetricsEnabled bool          `yaml:"detailed_metrics_enabled"`

	FinalSleep                      time.Duration `yaml:"final_sleep"`
	KeepInstanceInTheRingOnShutdown bool          `yaml:"keep_instance_in_the_ring_on_shutdown"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"hidden"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names"`
	InstancePort           int      `yaml:"instance_port" doc:"hidden"`
	InstanceAddr           string   `yaml:"instance_addr" doc:"hidden"`
	InstanceZone           string   `yaml:"instance_availability_zone"`

	NumTokens int `yaml:"num_tokens"`

	// Injected internally
	ListenPort      int           `yaml:"-"`
	RingCheckPeriod time.Duration `yaml:"-"`

	// Used for testing
	SkipUnregister bool `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("failed to get hostname, %w", err))
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("querier.ring.", "querier/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "querier.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "querier.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which rulers are considered unhealthy within the ring. 0 = never (timeout disabled).")
	f.DurationVar(&cfg.FinalSleep, "querier.ring.final-sleep", 0*time.Second, "The sleep seconds when ruler is shutting down. Need to be close to or larger than KV Store information propagation delay")
	f.IntVar(&cfg.ReplicationFactor, "querier.ring.replication-factor", 1, "EXPERIMENTAL: The replication factor to use when loading rule groups for API HA.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, "querier.ring.zone-awareness-enabled", false, "EXPERIMENTAL: True to enable zone-awareness and load rule groups across different availability zones for API HA.")
	f.StringVar(&cfg.TokensFilePath, "querier.ring.tokens-file-path", "", "EXPERIMENTAL: File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")
	f.BoolVar(&cfg.DetailedMetricsEnabled, "querier.ring.detailed-metrics-enabled", true, "Set to true to enable ring detailed metrics. These metrics provide detailed information, such as token count and ownership per tenant. Disabling them can significantly decrease the number of metrics emitted.")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "querier.ring.instance-interface-names", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "querier.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "querier.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "querier.ring.instance-id", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.InstanceZone, "querier.ring.instance-availability-zone", "", "The availability zone where this instance is running. Required if zone-awareness is enabled.")
	f.IntVar(&cfg.NumTokens, "querier.ring.num-tokens", 128, "Number of tokens for each ruler.")
	f.BoolVar(&cfg.KeepInstanceInTheRingOnShutdown, "querier.ring.keep-instance-in-the-ring-on-shutdown", false, "Keep instance in the ring on shut down.")
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.QuerierRing.RegisterFlags(f)

	f.StringVar(&cfg.SchedulerAddress, "querier.scheduler-address", "", "Hostname (and port) of scheduler that querier will periodically resolve, connect to and receive queries from. Only one of -querier.frontend-address or -querier.scheduler-address can be set. If neither is set, queries are only received via HTTP endpoint.")
	f.StringVar(&cfg.FrontendAddress, "querier.frontend-address", "", "Address of query frontend service, in host:port format. If -querier.scheduler-address is set as well, querier will use scheduler instead. Only one of -querier.frontend-address or -querier.scheduler-address can be set. If neither is set, queries are only received via HTTP endpoint.")

	f.DurationVar(&cfg.DNSLookupPeriod, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS for query-frontend or query-scheduler address.")

	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 10, "Number of simultaneous queries to process per query-frontend or query-scheduler.")
	f.BoolVar(&cfg.MatchMaxConcurrency, "querier.worker-match-max-concurrent", false, "Force worker concurrency to match the -querier.max-concurrent option. Overrides querier.worker-parallelism.")
	f.StringVar(&cfg.QuerierID, "querier.id", "", "Querier ID, sent to frontend service to identify requests from the same querier. Defaults to hostname.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", "", f)
}

func (cfg *Config) Validate(log log.Logger) error {
	if cfg.FrontendAddress != "" && cfg.SchedulerAddress != "" {
		return errors.New("frontend address and scheduler address are mutually exclusive, please use only one")
	}
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

	// notifyShutdown notifies the remote query-frontend or query-scheduler that the querier is
	// shutting down.
	notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string)
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

		ipAddr, err := ring.GetInstanceAddr(cfg.QuerierRing.InstanceAddr, cfg.QuerierRing.InstanceInterfaceNames, log)
		if err != nil {
			return nil, err
		}
		querierAddr := net.JoinHostPort(ipAddr, strconv.Itoa(cfg.QuerierRing.ListenPort))

		processor, servs = newSchedulerProcessor(cfg, handler, log, reg, querierAddr)

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

	// Empty address is only used in tests, where individual targets are added manually.
	if address != "" {
		w, err := util.NewDNSWatcher(address, cfg.DNSLookupPeriod, f)
		if err != nil {
			return nil, err
		}

		servs = append(servs, w)
	}

	if len(servs) > 0 {
		subservices, err := services.NewManager(servs...)
		if err != nil {
			return nil, errors.Wrap(err, "querier worker subservices")
		}

		f.subservices = subservices
	}

	f.BasicService = services.NewIdleService(f.starting, f.stopping)
	return f, nil
}

func (w *querierWorker) starting(ctx context.Context) error {
	if w.subservices == nil {
		return nil
	}
	return services.StartManagerAndAwaitHealthy(ctx, w.subservices)
}

func (w *querierWorker) stopping(_ error) error {
	// Stop all goroutines fetching queries. Note that in Stopping state,
	// worker no longer creates new managers in AddressAdded method.
	w.mu.Lock()
	for _, m := range w.managers {
		m.stop()
	}
	w.mu.Unlock()

	if w.subservices == nil {
		return nil
	}

	// Stop DNS watcher and services used by processor.
	return services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
}

func (w *querierWorker) AddressAdded(address string) {
	ctx := w.ServiceContext()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if m := w.managers[address]; m != nil {
		return
	}

	level.Info(w.log).Log("msg", "adding connection", "addr", address)
	conn, err := w.connect(address)
	if err != nil {
		level.Error(w.log).Log("msg", "error connecting", "addr", address, "err", err)
		return
	}

	w.managers[address] = newProcessorManager(ctx, w.processor, conn, address)
	// Called with lock.
	w.resetConcurrency()
}

func (w *querierWorker) AddressRemoved(address string) {
	level.Info(w.log).Log("msg", "removing connection", "addr", address)

	w.mu.Lock()
	p := w.managers[address]
	delete(w.managers, address)
	// Called with lock.
	w.resetConcurrency()
	w.mu.Unlock()

	if p != nil {
		p.stop()
	}
}

// Must be called with lock.
func (w *querierWorker) resetConcurrency() {
	totalConcurrency := 0
	index := 0

	for _, m := range w.managers {
		concurrency := 0

		if w.cfg.MatchMaxConcurrency {
			concurrency = w.cfg.MaxConcurrentRequests / len(w.managers)

			// If max concurrency does not evenly divide into our frontends a subset will be chosen
			// to receive an extra connection.  Frontend addresses were shuffled above so this will be a
			// random selection of frontends.
			if index < w.cfg.MaxConcurrentRequests%len(w.managers) {
				level.Warn(w.log).Log("msg", "max concurrency is not evenly divisible across targets, adding an extra connection", "addr", m.address)
				concurrency++
			}
		} else {
			concurrency = w.cfg.Parallelism
		}

		// If concurrency is 0 then MaxConcurrentRequests is less than the total number of
		// frontends/schedulers. In order to prevent accidentally starving a frontend or scheduler we are just going to
		// always connect once to every target.  This is dangerous b/c we may start exceeding PromQL
		// max concurrency.
		if concurrency == 0 {
			concurrency = 1
		}

		totalConcurrency += concurrency
		m.concurrency(concurrency)
		index++
	}

	if totalConcurrency > w.cfg.MaxConcurrentRequests {
		level.Warn(w.log).Log("msg", "total worker concurrency is greater than promql max concurrency. Queries may be queued in the querier which reduces QOS")
	}
}

func (w *querierWorker) connect(address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := w.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
