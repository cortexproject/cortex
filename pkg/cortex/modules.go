package cortex

import (
	"context"
	"flag"
	"fmt"
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/rules"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore"
	"github.com/cortexproject/cortex/pkg/api"
	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/compactor"
	configAPI "github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/flusher"
	"github.com/cortexproject/cortex/pkg/frontend"
	"github.com/cortexproject/cortex/pkg/frontend/transport"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/querier/tenantfederation"
	querier_worker "github.com/cortexproject/cortex/pkg/querier/worker"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/scheduler"
	"github.com/cortexproject/cortex/pkg/storegateway"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/modules"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// The various modules that make up Cortex.
const (
	API                      string = "api"
	Ring                     string = "ring"
	RuntimeConfig            string = "runtime-config"
	Overrides                string = "overrides"
	OverridesExporter        string = "overrides-exporter"
	Server                   string = "server"
	Distributor              string = "distributor"
	DistributorService       string = "distributor-service"
	Ingester                 string = "ingester"
	IngesterService          string = "ingester-service"
	Flusher                  string = "flusher"
	Querier                  string = "querier"
	Queryable                string = "queryable"
	StoreQueryable           string = "store-queryable"
	QueryFrontend            string = "query-frontend"
	QueryFrontendTripperware string = "query-frontend-tripperware"
	Store                    string = "store"
	DeleteRequestsStore      string = "delete-requests-store"
	TableManager             string = "table-manager"
	RulerStorage             string = "ruler-storage"
	Ruler                    string = "ruler"
	Configs                  string = "configs"
	AlertManager             string = "alertmanager"
	Compactor                string = "compactor"
	StoreGateway             string = "store-gateway"
	MemberlistKV             string = "memberlist-kv"
	TenantDeletion           string = "tenant-deletion"
	Purger                   string = "purger"
	QueryScheduler           string = "query-scheduler"
	TenantFederation         string = "tenant-federation"
	All                      string = "all"
)

func newDefaultConfig() *Config {
	defaultConfig := &Config{}
	defaultFS := flag.NewFlagSet("", flag.PanicOnError)
	defaultConfig.RegisterFlags(defaultFS)
	return defaultConfig
}

func (t *Cortex) initAPI() (services.Service, error) {
	t.Cfg.API.ServerPrefix = t.Cfg.Server.PathPrefix
	t.Cfg.API.LegacyHTTPPrefix = t.Cfg.HTTPPrefix

	a, err := api.New(t.Cfg.API, t.Cfg.Server, t.Server, util_log.Logger)
	if err != nil {
		return nil, err
	}

	t.API = a
	t.API.RegisterAPI(t.Cfg.Server.PathPrefix, t.Cfg, newDefaultConfig())

	return nil, nil
}

func (t *Cortex) initServer() (services.Service, error) {
	// Cortex handles signals on its own.
	DisableSignalHandling(&t.Cfg.Server)
	serv, err := server.New(t.Cfg.Server)
	if err != nil {
		return nil, err
	}

	t.Server = serv

	servicesToWaitFor := func() []services.Service {
		svs := []services.Service(nil)
		for m, s := range t.ServiceMap {
			// Server should not wait for itself.
			if m != Server {
				svs = append(svs, s)
			}
		}
		return svs
	}

	s := NewServerService(t.Server, servicesToWaitFor)

	return s, nil
}

func (t *Cortex) initRing() (serv services.Service, err error) {
	t.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Ring, err = ring.New(t.Cfg.Ingester.LifecyclerConfig.RingConfig, "ingester", ingester.RingKey, util_log.Logger, prometheus.WrapRegistererWithPrefix("cortex_", prometheus.DefaultRegisterer))
	if err != nil {
		return nil, err
	}

	t.API.RegisterRing(t.Ring)

	return t.Ring, nil
}

func (t *Cortex) initRuntimeConfig() (services.Service, error) {
	if t.Cfg.RuntimeConfig.LoadPath == "" {
		// no need to initialize module if load path is empty
		return nil, nil
	}
	t.Cfg.RuntimeConfig.Loader = loadRuntimeConfig

	// make sure to set default limits before we start loading configuration into memory
	validation.SetDefaultLimitsForYAMLUnmarshalling(t.Cfg.LimitsConfig)

	serv, err := runtimeconfig.New(t.Cfg.RuntimeConfig, prometheus.WrapRegistererWithPrefix("cortex_", prometheus.DefaultRegisterer), util_log.Logger)
	if err == nil {
		// TenantLimits just delegates to RuntimeConfig and doesn't have any state or need to do
		// anything in the start/stopping phase. Thus we can create it as part of runtime config
		// setup without any service instance of its own.
		t.TenantLimits = newTenantLimits(serv)
	}

	t.RuntimeConfig = serv
	t.API.RegisterRuntimeConfig(runtimeConfigHandler(t.RuntimeConfig, t.Cfg.LimitsConfig))
	return serv, err
}

func (t *Cortex) initOverrides() (serv services.Service, err error) {
	t.Overrides, err = validation.NewOverrides(t.Cfg.LimitsConfig, t.TenantLimits)
	// overrides don't have operational state, nor do they need to do anything more in starting/stopping phase,
	// so there is no need to return any service.
	return nil, err
}

func (t *Cortex) initOverridesExporter() (services.Service, error) {
	if t.Cfg.isModuleEnabled(OverridesExporter) && t.TenantLimits == nil {
		// This target isn't enabled by default ("all") and requires per-tenant limits to
		// work. Fail if it can't be setup correctly since the user explicitly wanted this
		// target to run.
		return nil, errors.New("overrides-exporter has been enabled, but no runtime configuration file was configured")
	}

	exporter := validation.NewOverridesExporter(t.TenantLimits)
	prometheus.MustRegister(exporter)

	// the overrides exporter has no state and reads overrides for runtime configuration each time it
	// is collected so there is no need to return any service
	return nil, nil
}

func (t *Cortex) initDistributorService() (serv services.Service, err error) {
	t.Cfg.Distributor.DistributorRing.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Distributor.ShuffleShardingLookbackPeriod = t.Cfg.Querier.ShuffleShardingIngestersLookbackPeriod

	// Check whether the distributor can join the distributors ring, which is
	// whenever it's not running as an internal dependency (ie. querier or
	// ruler's dependency)
	canJoinDistributorsRing := t.Cfg.isModuleEnabled(Distributor) || t.Cfg.isModuleEnabled(All)

	t.Distributor, err = distributor.New(t.Cfg.Distributor, t.Cfg.IngesterClient, t.Overrides, t.Ring, canJoinDistributorsRing, prometheus.DefaultRegisterer, util_log.Logger)
	if err != nil {
		return
	}

	return t.Distributor, nil
}

func (t *Cortex) initDistributor() (serv services.Service, err error) {
	t.API.RegisterDistributor(t.Distributor, t.Cfg.Distributor)

	return nil, nil
}

// initQueryable instantiates the queryable and promQL engine used to service queries to
// Cortex. It also registers the API endpoints associated with those two services.
func (t *Cortex) initQueryable() (serv services.Service, err error) {
	querierRegisterer := prometheus.WrapRegistererWith(prometheus.Labels{"engine": "querier"}, prometheus.DefaultRegisterer)

	// Create a querier queryable and PromQL engine
	t.QuerierQueryable, t.ExemplarQueryable, t.QuerierEngine = querier.New(t.Cfg.Querier, t.Overrides, t.Distributor, t.StoreQueryables, t.TombstonesLoader, querierRegisterer, util_log.Logger)

	// Register the default endpoints that are always enabled for the querier module
	t.API.RegisterQueryable(t.QuerierQueryable, t.Distributor)

	return nil, nil
}

// Enable merge querier if multi tenant query federation is enabled
func (t *Cortex) initTenantFederation() (serv services.Service, err error) {
	if t.Cfg.TenantFederation.Enabled {
		// Make sure the mergeQuerier is only used for request with more than a
		// single tenant. This allows for a less impactful enabling of tenant
		// federation.
		byPassForSingleQuerier := true
		t.QuerierQueryable = querier.NewSampleAndChunkQueryable(tenantfederation.NewQueryable(t.QuerierQueryable, byPassForSingleQuerier))
	}
	return nil, nil
}

// initQuerier registers an internal HTTP router with a Prometheus API backed by the
// Cortex Queryable. Then it does one of the following:
//
// 1. Query-Frontend Enabled: If Cortex has an All or QueryFrontend target, the internal
//    HTTP router is wrapped with Tenant ID parsing middleware and passed to the frontend
//    worker.
//
// 2. Querier Standalone: The querier will register the internal HTTP router with the external
//    HTTP router for the Prometheus API routes. Then the external HTTP server will be passed
//    as a http.Handler to the frontend worker.
//
// Route Diagram:
//
//                        │  query
//                        │ request
//                        │
//                        ▼
//              ┌──────────────────┐    QF to      ┌──────────────────┐
//              │  external HTTP   │    Worker     │                  │
//              │      router      │──────────────▶│ frontend worker  │
//              │                  │               │                  │
//              └──────────────────┘               └──────────────────┘
//                        │                                  │
//                                                           │
//               only in  │                                  │
//            microservice         ┌──────────────────┐      │
//              querier   │        │ internal Querier │      │
//                         ─ ─ ─ ─▶│      router      │◀─────┘
//                                 │                  │
//                                 └──────────────────┘
//                                           │
//                                           │
//  /metadata & /chunk ┌─────────────────────┼─────────────────────┐
//        requests     │                     │                     │
//                     │                     │                     │
//                     ▼                     ▼                     ▼
//           ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
//           │                  │  │                  │  │                  │
//           │Querier Queryable │  │  /api/v1 router  │  │ /api/prom router │
//           │                  │  │                  │  │                  │
//           └──────────────────┘  └──────────────────┘  └──────────────────┘
//                     ▲                     │                     │
//                     │                     └──────────┬──────────┘
//                     │                                ▼
//                     │                      ┌──────────────────┐
//                     │                      │                  │
//                     └──────────────────────│  Prometheus API  │
//                                            │                  │
//                                            └──────────────────┘
//
func (t *Cortex) initQuerier() (serv services.Service, err error) {
	// Create a internal HTTP handler that is configured with the Prometheus API routes and points
	// to a Prometheus API struct instantiated with the Cortex Queryable.
	internalQuerierRouter := api.NewQuerierHandler(
		t.Cfg.API,
		t.QuerierQueryable,
		t.ExemplarQueryable,
		t.QuerierEngine,
		t.Distributor,
		t.TombstonesLoader,
		prometheus.DefaultRegisterer,
		util_log.Logger,
	)

	// If the querier is running standalone without the query-frontend or query-scheduler, we must register it's internal
	// HTTP handler externally and provide the external Cortex Server HTTP handler to the frontend worker
	// to ensure requests it processes use the default middleware instrumentation.
	if !t.Cfg.isModuleEnabled(QueryFrontend) && !t.Cfg.isModuleEnabled(QueryScheduler) && !t.Cfg.isModuleEnabled(All) {
		// First, register the internal querier handler with the external HTTP server
		t.API.RegisterQueryAPI(internalQuerierRouter)

		// Second, set the http.Handler that the frontend worker will use to process requests to point to
		// the external HTTP server. This will allow the querier to consolidate query metrics both external
		// and internal using the default instrumentation when running as a standalone service.
		internalQuerierRouter = t.Server.HTTPServer.Handler
	} else {
		// Single binary mode requires a query frontend endpoint for the worker. If no frontend and scheduler endpoint
		// is configured, Cortex will default to using frontend on localhost on it's own GRPC listening port.
		if t.Cfg.Worker.FrontendAddress == "" && t.Cfg.Worker.SchedulerAddress == "" {
			address := fmt.Sprintf("127.0.0.1:%d", t.Cfg.Server.GRPCListenPort)
			level.Warn(util_log.Logger).Log("msg", "Worker address is empty in single binary mode.  Attempting automatic worker configuration.  If queries are unresponsive consider configuring the worker explicitly.", "address", address)
			t.Cfg.Worker.FrontendAddress = address
		}

		// Add a middleware to extract the trace context and add a header.
		internalQuerierRouter = nethttp.MiddlewareFunc(opentracing.GlobalTracer(), internalQuerierRouter.ServeHTTP, nethttp.OperationNameFunc(func(r *http.Request) string {
			return "internalQuerier"
		}))

		// If queries are processed using the external HTTP Server, we need wrap the internal querier with
		// HTTP router with middleware to parse the tenant ID from the HTTP header and inject it into the
		// request context.
		internalQuerierRouter = t.API.AuthMiddleware.Wrap(internalQuerierRouter)
	}

	// If neither frontend address or scheduler address is configured, no worker is needed.
	if t.Cfg.Worker.FrontendAddress == "" && t.Cfg.Worker.SchedulerAddress == "" {
		return nil, nil
	}

	t.Cfg.Worker.MaxConcurrentRequests = t.Cfg.Querier.MaxConcurrent
	return querier_worker.NewQuerierWorker(t.Cfg.Worker, httpgrpc_server.NewServer(internalQuerierRouter), util_log.Logger, prometheus.DefaultRegisterer)
}

func (t *Cortex) initStoreQueryables() (services.Service, error) {
	var servs []services.Service

	//nolint:golint // I prefer this form over removing 'else', because it allows q to have smaller scope.
	if q, err := initQueryableForEngine(t.Cfg.Storage.Engine, t.Cfg, t.Overrides, prometheus.DefaultRegisterer); err != nil {
		return nil, fmt.Errorf("failed to initialize querier for engine '%s': %v", t.Cfg.Storage.Engine, err)
	} else {
		t.StoreQueryables = append(t.StoreQueryables, querier.UseAlwaysQueryable(q))
		if s, ok := q.(services.Service); ok {
			servs = append(servs, s)
		}
	}

	// Return service, if any.
	switch len(servs) {
	case 0:
		return nil, nil
	case 1:
		return servs[0], nil
	default:
		// TODO cleanup.
		// No need to support this case yet.
		// When we get there, we will need a wrapper service, that starts all subservices, and will also monitor them for failures.
		// Not difficult, but also not necessary right now.
		return nil, fmt.Errorf("too many services")
	}
}

func initQueryableForEngine(engine string, cfg Config, limits *validation.Overrides, reg prometheus.Registerer) (prom_storage.Queryable, error) {
	switch engine {
	case storage.StorageEngineBlocks:
		// When running in single binary, if the blocks sharding is disabled and no custom
		// store-gateway address has been configured, we can set it to the running process.
		if cfg.isModuleEnabled(All) && !cfg.StoreGateway.ShardingEnabled && cfg.Querier.StoreGatewayAddresses == "" {
			cfg.Querier.StoreGatewayAddresses = fmt.Sprintf("127.0.0.1:%d", cfg.Server.GRPCListenPort)
		}

		return querier.NewBlocksStoreQueryableFromConfig(cfg.Querier, cfg.StoreGateway, cfg.BlocksStorage, limits, util_log.Logger, reg)

	default:
		return nil, fmt.Errorf("unknown storage engine '%s'", engine)
	}
}

func (t *Cortex) tsdbIngesterConfig() {
	t.Cfg.Ingester.BlocksStorageEnabled = t.Cfg.Storage.Engine == storage.StorageEngineBlocks
	t.Cfg.Ingester.BlocksStorageConfig = t.Cfg.BlocksStorage
}

func (t *Cortex) initIngesterService() (serv services.Service, err error) {
	t.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Ingester.LifecyclerConfig.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Ingester.DistributorShardingStrategy = t.Cfg.Distributor.ShardingStrategy
	t.Cfg.Ingester.DistributorShardByAllLabels = t.Cfg.Distributor.ShardByAllLabels
	t.Cfg.Ingester.StreamTypeFn = ingesterChunkStreaming(t.RuntimeConfig)
	t.Cfg.Ingester.InstanceLimitsFn = ingesterInstanceLimits(t.RuntimeConfig)
	t.tsdbIngesterConfig()

	t.Ingester, err = ingester.New(t.Cfg.Ingester, t.Overrides, prometheus.DefaultRegisterer, util_log.Logger)
	if err != nil {
		return
	}

	return t.Ingester, nil
}

func (t *Cortex) initIngester() (serv services.Service, err error) {
	t.API.RegisterIngester(t.Ingester, t.Cfg.Distributor)

	return nil, nil
}

func (t *Cortex) initFlusher() (serv services.Service, err error) {
	t.tsdbIngesterConfig()

	t.Flusher, err = flusher.New(
		t.Cfg.Flusher,
		t.Cfg.Ingester,
		t.Overrides,
		prometheus.DefaultRegisterer,
		util_log.Logger,
	)
	if err != nil {
		return
	}

	return t.Flusher, nil
}

func (t *Cortex) initChunkStore() (serv services.Service, err error) {
	if t.Cfg.Storage.Engine == storage.StorageEngineChunks {
		return nil, errors.New("should not get here: ingesting into chunks storage is no longer supported")
	}
	return nil, nil
}

func (t *Cortex) initDeleteRequestsStore() (serv services.Service, err error) {
	// no-op while blocks store does not support series deletion
	t.TombstonesLoader = purger.NewNoopTombstonesLoader()
	return
}

// initQueryFrontendTripperware instantiates the tripperware used by the query frontend
// to optimize Prometheus query requests.
func (t *Cortex) initQueryFrontendTripperware() (serv services.Service, err error) {
	tripperware, cache, err := queryrange.NewTripperware(
		t.Cfg.QueryRange,
		util_log.Logger,
		t.Overrides,
		queryrange.PrometheusCodec,
		queryrange.PrometheusResponseExtractor{},
		prometheus.DefaultRegisterer,
		t.TombstonesLoader,
	)

	if err != nil {
		return nil, err
	}

	t.QueryFrontendTripperware = tripperware

	return services.NewIdleService(nil, func(_ error) error {
		if cache != nil {
			cache.Stop()
			cache = nil
		}
		return nil
	}), nil
}

func (t *Cortex) initQueryFrontend() (serv services.Service, err error) {
	roundTripper, frontendV1, frontendV2, err := frontend.InitFrontend(t.Cfg.Frontend, t.Overrides, t.Cfg.Server.GRPCListenPort, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	// Wrap roundtripper into Tripperware.
	roundTripper = t.QueryFrontendTripperware(roundTripper)

	handler := transport.NewHandler(t.Cfg.Frontend.Handler, roundTripper, util_log.Logger, prometheus.DefaultRegisterer)
	t.API.RegisterQueryFrontendHandler(handler)

	if frontendV1 != nil {
		t.API.RegisterQueryFrontend1(frontendV1)
		t.Frontend = frontendV1

		return frontendV1, nil
	} else if frontendV2 != nil {
		t.API.RegisterQueryFrontend2(frontendV2)

		return frontendV2, nil
	}

	return nil, nil
}

func (t *Cortex) initRulerStorage() (serv services.Service, err error) {
	// if the ruler is not configured and we're in single binary then let's just log an error and continue.
	// unfortunately there is no way to generate a "default" config and compare default against actual
	// to determine if it's unconfigured.  the following check, however, correctly tests this.
	// Single binary integration tests will break if this ever drifts
	if t.Cfg.isModuleEnabled(All) && t.Cfg.RulerStorage.IsDefaults() {
		level.Info(util_log.Logger).Log("msg", "Ruler storage is not configured in single binary mode and will not be started.")
		return
	}

	t.RulerStorage, err = ruler.NewRuleStore(context.Background(), t.Cfg.RulerStorage, t.Overrides, rules.FileLoader{}, util_log.Logger, prometheus.DefaultRegisterer)
	return
}

func (t *Cortex) initRuler() (serv services.Service, err error) {
	if t.RulerStorage == nil {
		level.Info(util_log.Logger).Log("msg", "RulerStorage is nil.  Not starting the ruler.")
		return nil, nil
	}

	t.Cfg.Ruler.Ring.ListenPort = t.Cfg.Server.GRPCListenPort
	rulerRegisterer := prometheus.WrapRegistererWith(prometheus.Labels{"engine": "ruler"}, prometheus.DefaultRegisterer)
	// TODO: Consider wrapping logger to differentiate from querier module logger
	queryable, _, engine := querier.New(t.Cfg.Querier, t.Overrides, t.Distributor, t.StoreQueryables, t.TombstonesLoader, rulerRegisterer, util_log.Logger)

	managerFactory := ruler.DefaultTenantManagerFactory(t.Cfg.Ruler, t.Distributor, queryable, engine, t.Overrides, prometheus.DefaultRegisterer)
	manager, err := ruler.NewDefaultMultiTenantManager(t.Cfg.Ruler, managerFactory, prometheus.DefaultRegisterer, util_log.Logger)
	if err != nil {
		return nil, err
	}

	t.Ruler, err = ruler.NewRuler(
		t.Cfg.Ruler,
		manager,
		prometheus.DefaultRegisterer,
		util_log.Logger,
		t.RulerStorage,
		t.Overrides,
	)
	if err != nil {
		return
	}

	// Expose HTTP/GRPC endpoints for the Ruler service
	t.API.RegisterRuler(t.Ruler)

	// If the API is enabled, register the Ruler API
	if t.Cfg.Ruler.EnableAPI {
		t.API.RegisterRulerAPI(ruler.NewAPI(t.Ruler, t.RulerStorage, util_log.Logger))
	}

	return t.Ruler, nil
}

func (t *Cortex) initConfig() (serv services.Service, err error) {
	t.ConfigDB, err = db.New(t.Cfg.Configs.DB)
	if err != nil {
		return
	}

	t.ConfigAPI = configAPI.New(t.ConfigDB, t.Cfg.Configs.API)
	t.ConfigAPI.RegisterRoutes(t.Server.HTTP)
	return services.NewIdleService(nil, func(_ error) error {
		t.ConfigDB.Close()
		return nil
	}), nil
}

func (t *Cortex) initAlertManager() (serv services.Service, err error) {
	t.Cfg.Alertmanager.ShardingRing.ListenPort = t.Cfg.Server.GRPCListenPort

	// Initialise the store.
	store, err := alertstore.NewAlertStore(context.Background(), t.Cfg.AlertmanagerStorage, t.Overrides, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	t.Alertmanager, err = alertmanager.NewMultitenantAlertmanager(&t.Cfg.Alertmanager, store, t.Overrides, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	t.API.RegisterAlertmanager(t.Alertmanager, t.Cfg.isModuleEnabled(AlertManager), t.Cfg.Alertmanager.EnableAPI)
	return t.Alertmanager, nil
}

func (t *Cortex) initCompactor() (serv services.Service, err error) {
	t.Cfg.Compactor.ShardingRing.ListenPort = t.Cfg.Server.GRPCListenPort

	t.Compactor, err = compactor.NewCompactor(t.Cfg.Compactor, t.Cfg.BlocksStorage, t.Overrides, util_log.Logger, prometheus.DefaultRegisterer, t.Overrides)
	if err != nil {
		return
	}

	// Expose HTTP endpoints.
	t.API.RegisterCompactor(t.Compactor)
	return t.Compactor, nil
}

func (t *Cortex) initStoreGateway() (serv services.Service, err error) {
	if t.Cfg.Storage.Engine != storage.StorageEngineBlocks {
		if !t.Cfg.isModuleEnabled(All) {
			return nil, fmt.Errorf("storage engine must be set to blocks to enable the store-gateway")
		}
		return nil, nil
	}

	t.Cfg.StoreGateway.ShardingRing.ListenPort = t.Cfg.Server.GRPCListenPort

	t.StoreGateway, err = storegateway.NewStoreGateway(t.Cfg.StoreGateway, t.Cfg.BlocksStorage, t.Overrides, t.Cfg.Server.LogLevel, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	// Expose HTTP endpoints.
	t.API.RegisterStoreGateway(t.StoreGateway)

	return t.StoreGateway, nil
}

func (t *Cortex) initMemberlistKV() (services.Service, error) {
	reg := prometheus.DefaultRegisterer
	t.Cfg.MemberlistKV.MetricsRegisterer = reg
	t.Cfg.MemberlistKV.Codecs = []codec.Codec{
		ring.GetCodec(),
	}
	dnsProviderReg := prometheus.WrapRegistererWithPrefix(
		"cortex_",
		prometheus.WrapRegistererWith(
			prometheus.Labels{"name": "memberlist"},
			reg,
		),
	)
	dnsProvider := dns.NewProvider(util_log.Logger, dnsProviderReg, dns.GolangResolverType)
	t.MemberlistKV = memberlist.NewKVInitService(&t.Cfg.MemberlistKV, util_log.Logger, dnsProvider, reg)
	t.API.RegisterMemberlistKV(t.MemberlistKV)

	// Update the config.
	t.Cfg.Distributor.DistributorRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.StoreGateway.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Compactor.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Ruler.Ring.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Alertmanager.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV

	return t.MemberlistKV, nil
}

func (t *Cortex) initTenantDeletionAPI() (services.Service, error) {
	if t.Cfg.Storage.Engine != storage.StorageEngineBlocks {
		return nil, nil
	}

	// t.RulerStorage can be nil when running in single-binary mode, and rule storage is not configured.
	tenantDeletionAPI, err := purger.NewTenantDeletionAPI(t.Cfg.BlocksStorage, t.Overrides, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	t.API.RegisterTenantDeletion(tenantDeletionAPI)
	return nil, nil
}

func (t *Cortex) initQueryScheduler() (services.Service, error) {
	s, err := scheduler.NewScheduler(t.Cfg.QueryScheduler, t.Overrides, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, errors.Wrap(err, "query-scheduler init")
	}

	t.API.RegisterQueryScheduler(s)
	return s, nil
}

func (t *Cortex) setupModuleManager() error {
	mm := modules.NewManager(util_log.Logger)

	// Register all modules here.
	// RegisterModule(name string, initFn func()(services.Service, error))
	mm.RegisterModule(Server, t.initServer, modules.UserInvisibleModule)
	mm.RegisterModule(API, t.initAPI, modules.UserInvisibleModule)
	mm.RegisterModule(RuntimeConfig, t.initRuntimeConfig, modules.UserInvisibleModule)
	mm.RegisterModule(MemberlistKV, t.initMemberlistKV, modules.UserInvisibleModule)
	mm.RegisterModule(Ring, t.initRing, modules.UserInvisibleModule)
	mm.RegisterModule(Overrides, t.initOverrides, modules.UserInvisibleModule)
	mm.RegisterModule(OverridesExporter, t.initOverridesExporter)
	mm.RegisterModule(Distributor, t.initDistributor)
	mm.RegisterModule(DistributorService, t.initDistributorService, modules.UserInvisibleModule)
	mm.RegisterModule(Store, t.initChunkStore, modules.UserInvisibleModule)
	mm.RegisterModule(DeleteRequestsStore, t.initDeleteRequestsStore, modules.UserInvisibleModule)
	mm.RegisterModule(Ingester, t.initIngester)
	mm.RegisterModule(IngesterService, t.initIngesterService, modules.UserInvisibleModule)
	mm.RegisterModule(Flusher, t.initFlusher)
	mm.RegisterModule(Queryable, t.initQueryable, modules.UserInvisibleModule)
	mm.RegisterModule(Querier, t.initQuerier)
	mm.RegisterModule(StoreQueryable, t.initStoreQueryables, modules.UserInvisibleModule)
	mm.RegisterModule(QueryFrontendTripperware, t.initQueryFrontendTripperware, modules.UserInvisibleModule)
	mm.RegisterModule(QueryFrontend, t.initQueryFrontend)
	mm.RegisterModule(RulerStorage, t.initRulerStorage, modules.UserInvisibleModule)
	mm.RegisterModule(Ruler, t.initRuler)
	mm.RegisterModule(Configs, t.initConfig)
	mm.RegisterModule(AlertManager, t.initAlertManager)
	mm.RegisterModule(Compactor, t.initCompactor)
	mm.RegisterModule(StoreGateway, t.initStoreGateway)
	mm.RegisterModule(TenantDeletion, t.initTenantDeletionAPI, modules.UserInvisibleModule)
	mm.RegisterModule(Purger, nil)
	mm.RegisterModule(QueryScheduler, t.initQueryScheduler)
	mm.RegisterModule(TenantFederation, t.initTenantFederation, modules.UserInvisibleModule)
	mm.RegisterModule(All, nil)

	// Add dependencies
	deps := map[string][]string{
		API:                      {Server},
		MemberlistKV:             {API},
		RuntimeConfig:            {API},
		Ring:                     {API, RuntimeConfig, MemberlistKV},
		Overrides:                {RuntimeConfig},
		OverridesExporter:        {RuntimeConfig},
		Distributor:              {DistributorService, API},
		DistributorService:       {Ring, Overrides},
		Store:                    {Overrides, DeleteRequestsStore},
		Ingester:                 {IngesterService, API},
		IngesterService:          {Overrides, Store, RuntimeConfig, MemberlistKV},
		Flusher:                  {Store, API},
		Queryable:                {Overrides, DistributorService, Store, Ring, API, StoreQueryable, MemberlistKV},
		Querier:                  {TenantFederation},
		StoreQueryable:           {Overrides, Store, MemberlistKV},
		QueryFrontendTripperware: {API, Overrides, DeleteRequestsStore},
		QueryFrontend:            {QueryFrontendTripperware},
		QueryScheduler:           {API, Overrides},
		Ruler:                    {DistributorService, Store, StoreQueryable, RulerStorage},
		RulerStorage:             {Overrides},
		Configs:                  {API},
		AlertManager:             {API, MemberlistKV, Overrides},
		Compactor:                {API, MemberlistKV, Overrides},
		StoreGateway:             {API, Overrides, MemberlistKV},
		TenantDeletion:           {Store, API, Overrides},
		Purger:                   {TenantDeletion},
		TenantFederation:         {Queryable},
		All:                      {QueryFrontend, Querier, Ingester, Distributor, Purger, StoreGateway, Ruler},
	}
	for mod, targets := range deps {
		if err := mm.AddDependency(mod, targets...); err != nil {
			return err
		}
	}

	t.ModuleManager = mm

	return nil
}
