package cortex

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/compactor"
	"github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/push"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type moduleName int

// The various modules that make up Cortex.
const (
	Ring moduleName = iota
	RuntimeConfig
	Overrides
	Server
	Distributor
	Ingester
	Querier
	StoreQueryable
	QueryFrontend
	Store
	TableManager
	Ruler
	Configs
	AlertManager
	Compactor
	MemberlistKV
	DataPurger
	All
)

func (m moduleName) String() string {
	switch m {
	case Ring:
		return "ring"
	case RuntimeConfig:
		return "runtime-config"
	case Overrides:
		return "overrides"
	case Server:
		return "server"
	case Distributor:
		return "distributor"
	case Store:
		return "store"
	case Ingester:
		return "ingester"
	case Querier:
		return "querier"
	case StoreQueryable:
		return "store-queryable"
	case QueryFrontend:
		return "query-frontend"
	case TableManager:
		return "table-manager"
	case Ruler:
		return "ruler"
	case Configs:
		return "configs"
	case AlertManager:
		return "alertmanager"
	case Compactor:
		return "compactor"
	case MemberlistKV:
		return "memberlist-kv"
	case DataPurger:
		return "data-purger"
	case All:
		return "all"
	default:
		panic(fmt.Sprintf("unknown module name: %d", m))
	}
}

func (m *moduleName) Set(s string) error {
	switch strings.ToLower(s) {
	case "ring":
		*m = Ring
		return nil
	case "overrides":
		*m = Overrides
		return nil
	case "server":
		*m = Server
		return nil
	case "distributor":
		*m = Distributor
		return nil
	case "store":
		*m = Store
		return nil
	case "ingester":
		*m = Ingester
		return nil
	case "querier":
		*m = Querier
		return nil
	case "store-queryable":
		*m = StoreQueryable
		return nil
	case "query-frontend":
		*m = QueryFrontend
		return nil
	case "table-manager":
		*m = TableManager
		return nil
	case "ruler":
		*m = Ruler
		return nil
	case "configs":
		*m = Configs
		return nil
	case "alertmanager":
		*m = AlertManager
		return nil
	case "compactor":
		*m = Compactor
		return nil
	case "data-purger":
		*m = DataPurger
		return nil
	case "all":
		*m = All
		return nil
	default:
		return fmt.Errorf("unrecognised module name: %s", s)
	}
}

func (m moduleName) MarshalYAML() (interface{}, error) {
	return m.String(), nil
}

func (m *moduleName) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return m.Set(s)
}

func (t *Cortex) initServer(cfg *Config) (services.Service, error) {
	serv, err := server.New(cfg.Server)
	if err != nil {
		return nil, err
	}

	t.server = serv

	servicesToWaitFor := func() []services.Service {
		svs := []services.Service(nil)
		for m, s := range t.serviceMap {
			// Server should not wait for itself.
			if m != Server {
				svs = append(svs, s)
			}
		}
		return svs
	}

	s := NewServerService(cfg, t.server, servicesToWaitFor)
	serv.HTTP.HandleFunc("/", s.indexHandler)
	serv.HTTP.HandleFunc("/config", s.configHandler)

	return s, nil
}

func (t *Cortex) initRing(cfg *Config) (serv services.Service, err error) {
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.runtimeConfig)
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.memberlistKV.GetMemberlistKV
	t.ring, err = ring.New(cfg.Ingester.LifecyclerConfig.RingConfig, "ingester", ring.IngesterRingKey)
	if err != nil {
		return nil, err
	}
	prometheus.MustRegister(t.ring)
	t.server.HTTP.Handle("/ring", t.ring)
	return t.ring, nil
}

func (t *Cortex) initRuntimeConfig(cfg *Config) (services.Service, error) {
	if cfg.RuntimeConfig.LoadPath == "" {
		cfg.RuntimeConfig.LoadPath = cfg.LimitsConfig.PerTenantOverrideConfig
		cfg.RuntimeConfig.ReloadPeriod = cfg.LimitsConfig.PerTenantOverridePeriod
	}
	cfg.RuntimeConfig.Loader = loadRuntimeConfig

	// make sure to set default limits before we start loading configuration into memory
	validation.SetDefaultLimitsForYAMLUnmarshalling(cfg.LimitsConfig)

	serv, err := runtimeconfig.NewRuntimeConfigManager(cfg.RuntimeConfig, prometheus.DefaultRegisterer)
	t.runtimeConfig = serv
	return serv, err
}

func (t *Cortex) initOverrides(cfg *Config) (serv services.Service, err error) {
	t.overrides, err = validation.NewOverrides(cfg.LimitsConfig, tenantLimitsFromRuntimeConfig(t.runtimeConfig))
	// overrides don't have operational state, nor do they need to do anything more in starting/stopping phase,
	// so there is no need to return any service.
	return nil, err
}

func (t *Cortex) initDistributor(cfg *Config) (serv services.Service, err error) {
	cfg.Distributor.DistributorRing.ListenPort = cfg.Server.GRPCListenPort
	cfg.Distributor.DistributorRing.KVStore.MemberlistKV = t.memberlistKV.GetMemberlistKV

	// Check whether the distributor can join the distributors ring, which is
	// whenever it's not running as an internal dependency (ie. querier or
	// ruler's dependency)
	canJoinDistributorsRing := (cfg.Target == All || cfg.Target == Distributor)

	t.distributor, err = distributor.New(cfg.Distributor, cfg.IngesterClient, t.overrides, t.ring, canJoinDistributorsRing)
	if err != nil {
		return
	}

	t.server.HTTP.HandleFunc("/all_user_stats", t.distributor.AllUserStatsHandler)
	t.server.HTTP.Handle("/api/prom/push", t.httpAuthMiddleware.Wrap(push.Handler(cfg.Distributor, t.distributor.Push)))
	t.server.HTTP.Handle("/ha-tracker", t.distributor.Replicas)
	return t.distributor, nil
}

func (t *Cortex) initQuerier(cfg *Config) (serv services.Service, err error) {
	queryable, engine := querier.New(cfg.Querier, t.distributor, t.storeQueryable)
	api := v1.NewAPI(
		engine,
		queryable,
		querier.DummyTargetRetriever{},
		querier.DummyAlertmanagerRetriever{},
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		func() v1.TSDBAdmin { return nil }, // Only needed for admin APIs.
		false,                              // Disable admin APIs.
		util.Logger,
		querier.DummyRulesRetriever{},
		0, 0, 0, // Remote read samples and concurrency limit.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
	)
	promRouter := route.New().WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	subrouter := t.server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(fakeRemoteAddr(t.httpAuthMiddleware.Wrap(promRouter)))
	subrouter.Path("/read").Handler(t.httpAuthMiddleware.Wrap(querier.RemoteReadHandler(queryable)))
	subrouter.Path("/chunks").Handler(t.httpAuthMiddleware.Wrap(querier.ChunksHandler(queryable)))
	subrouter.Path("/user_stats").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(t.distributor.UserStatsHandler)))

	// Start the query frontend worker once the query engine and the store
	// have been successfully initialized.
	t.worker, err = frontend.NewWorker(cfg.Worker, httpgrpc_server.NewServer(t.server.HTTPServer.Handler), util.Logger)
	if err != nil {
		return
	}

	// TODO: If queryable returned from querier.New was a service, it could actually wait for storeQueryable
	// (if it also implemented Service) to finish starting... and return error if it's not in Running state.
	// This requires extra work, which is out of scope for this proof-of-concept...
	// BUT this extra functionality is ONE OF THE REASONS to introduce entire "Services" concept into Cortex.
	// For now, only return service that stops the worker, and Querier will be used even before storeQueryable has finished starting.

	return services.NewIdleService(nil, func(_ error) error {
		t.worker.Stop()
		return nil
	}), nil
}

// Latest Prometheus requires r.RemoteAddr to be set to addr:port, otherwise it reject the request.
// Requests to Querier sometimes doesn't have that (if they are fetched from Query-Frontend).
// Prometheus uses this when logging queries to QueryLogger, but Cortex doesn't call engine.SetQueryLogger to set one.
//
// Can be removed when (if) https://github.com/prometheus/prometheus/pull/6840 is merged.
func fakeRemoteAddr(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RemoteAddr == "" {
			r.RemoteAddr = "127.0.0.1:8888"
		}
		handler.ServeHTTP(w, r)
	})
}

func (t *Cortex) initStoreQueryable(cfg *Config) (services.Service, error) {
	if cfg.Storage.Engine == storage.StorageEngineChunks {
		t.storeQueryable = querier.NewChunkStoreQueryable(cfg.Querier, t.store)
		return nil, nil
	}

	if cfg.Storage.Engine == storage.StorageEngineTSDB {
		storeQueryable, err := querier.NewBlockQueryable(cfg.TSDB, cfg.Server.LogLevel, prometheus.DefaultRegisterer)
		if err != nil {
			return nil, err
		}
		t.storeQueryable = storeQueryable
		return storeQueryable, nil
	}

	return nil, fmt.Errorf("unknown storage engine '%s'", cfg.Storage.Engine)
}

func (t *Cortex) initIngester(cfg *Config) (serv services.Service, err error) {
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.runtimeConfig)
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.memberlistKV.GetMemberlistKV
	cfg.Ingester.LifecyclerConfig.ListenPort = &cfg.Server.GRPCListenPort
	cfg.Ingester.TSDBEnabled = cfg.Storage.Engine == storage.StorageEngineTSDB
	cfg.Ingester.TSDBConfig = cfg.TSDB
	cfg.Ingester.ShardByAllLabels = cfg.Distributor.ShardByAllLabels

	t.ingester, err = ingester.New(cfg.Ingester, cfg.IngesterClient, t.overrides, t.store, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	client.RegisterIngesterServer(t.server.GRPC, t.ingester)
	grpc_health_v1.RegisterHealthServer(t.server.GRPC, t.ingester)
	t.server.HTTP.Path("/flush").Handler(http.HandlerFunc(t.ingester.FlushHandler))
	t.server.HTTP.Path("/shutdown").Handler(http.HandlerFunc(t.ingester.ShutdownHandler))
	t.server.HTTP.Handle("/push", t.httpAuthMiddleware.Wrap(push.Handler(cfg.Distributor, t.ingester.Push)))
	return t.ingester, nil
}

func (t *Cortex) initStore(cfg *Config) (serv services.Service, err error) {
	if cfg.Storage.Engine == storage.StorageEngineTSDB {
		return nil, nil
	}
	err = cfg.Schema.Load()
	if err != nil {
		return
	}

	t.store, err = storage.NewStore(cfg.Storage, cfg.ChunkStore, cfg.Schema, t.overrides)
	if err != nil {
		return
	}

	return services.NewIdleService(nil, func(_ error) error {
		t.store.Stop()
		return nil
	}), nil
}

func (t *Cortex) initQueryFrontend(cfg *Config) (serv services.Service, err error) {
	// Load the schema only if sharded queries is set.
	if cfg.QueryRange.ShardedQueries {
		err = cfg.Schema.Load()
		if err != nil {
			return
		}
	}

	t.frontend, err = frontend.New(cfg.Frontend, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}
	tripperware, cache, err := queryrange.NewTripperware(
		cfg.QueryRange,
		util.Logger,
		t.overrides,
		queryrange.PrometheusCodec,
		queryrange.PrometheusResponseExtractor,
		cfg.Schema,
		promql.EngineOpts{
			Logger:     util.Logger,
			Reg:        prometheus.DefaultRegisterer,
			MaxSamples: cfg.Querier.MaxSamples,
			Timeout:    cfg.Querier.Timeout,
		},
		cfg.Querier.QueryIngestersWithin,
		prometheus.DefaultRegisterer,
	)

	if err != nil {
		return nil, err
	}
	t.cache = cache
	t.frontend.Wrap(tripperware)

	frontend.RegisterFrontendServer(t.server.GRPC, t.frontend)
	t.server.HTTP.PathPrefix(cfg.HTTPPrefix).Handler(
		t.httpAuthMiddleware.Wrap(
			t.frontend.Handler(),
		),
	)
	return services.NewIdleService(nil, func(_ error) error {
		t.frontend.Close()
		if t.cache != nil {
			t.cache.Stop()
			t.cache = nil
		}
		return nil
	}), nil
}

func (t *Cortex) initTableManager(cfg *Config) (services.Service, error) {
	if cfg.Storage.Engine == storage.StorageEngineTSDB {
		return nil, nil // table manager isn't used in v2
	}

	err := cfg.Schema.Load()
	if err != nil {
		return nil, err
	}

	// Assume the newest config is the one to use
	lastConfig := &cfg.Schema.Configs[len(cfg.Schema.Configs)-1]

	if (cfg.TableManager.ChunkTables.WriteScale.Enabled ||
		cfg.TableManager.IndexTables.WriteScale.Enabled ||
		cfg.TableManager.ChunkTables.InactiveWriteScale.Enabled ||
		cfg.TableManager.IndexTables.InactiveWriteScale.Enabled ||
		cfg.TableManager.ChunkTables.ReadScale.Enabled ||
		cfg.TableManager.IndexTables.ReadScale.Enabled ||
		cfg.TableManager.ChunkTables.InactiveReadScale.Enabled ||
		cfg.TableManager.IndexTables.InactiveReadScale.Enabled) &&
		(cfg.Storage.AWSStorageConfig.ApplicationAutoScaling.URL == nil && cfg.Storage.AWSStorageConfig.Metrics.URL == "") {
		level.Error(util.Logger).Log("msg", "WriteScale is enabled but no ApplicationAutoScaling or Metrics URL has been provided")
		os.Exit(1)
	}

	tableClient, err := storage.NewTableClient(lastConfig.IndexType, cfg.Storage)
	if err != nil {
		return nil, err
	}

	bucketClient, err := storage.NewBucketClient(cfg.Storage)
	util.CheckFatal("initializing bucket client", err)

	t.tableManager, err = chunk.NewTableManager(cfg.TableManager, cfg.Schema, cfg.Ingester.MaxChunkAge, tableClient, bucketClient, prometheus.DefaultRegisterer)
	return t.tableManager, err
}

func (t *Cortex) initRuler(cfg *Config) (serv services.Service, err error) {
	cfg.Ruler.Ring.ListenPort = cfg.Server.GRPCListenPort
	cfg.Ruler.Ring.KVStore.MemberlistKV = t.memberlistKV.GetMemberlistKV
	queryable, engine := querier.New(cfg.Querier, t.distributor, t.storeQueryable)

	t.ruler, err = ruler.NewRuler(cfg.Ruler, engine, queryable, t.distributor, prometheus.DefaultRegisterer, util.Logger)
	if err != nil {
		return
	}

	if cfg.Ruler.EnableAPI {
		subrouter := t.server.HTTP.PathPrefix(cfg.HTTPPrefix).Subrouter()
		t.ruler.RegisterRoutes(subrouter)
		ruler.RegisterRulerServer(t.server.GRPC, t.ruler)
	}

	t.server.HTTP.Handle("/ruler_ring", t.ruler)
	return t.ruler, nil
}

func (t *Cortex) initConfig(cfg *Config) (serv services.Service, err error) {
	t.configDB, err = db.New(cfg.Configs.DB)
	if err != nil {
		return
	}

	t.configAPI = api.New(t.configDB, cfg.Configs.API)
	t.configAPI.RegisterRoutes(t.server.HTTP)
	return services.NewIdleService(nil, func(_ error) error {
		t.configDB.Close()
		return nil
	}), nil
}

func (t *Cortex) initAlertManager(cfg *Config) (serv services.Service, err error) {
	t.alertmanager, err = alertmanager.NewMultitenantAlertmanager(&cfg.Alertmanager, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}
	t.server.HTTP.PathPrefix("/status").Handler(t.alertmanager.GetStatusHandler())

	// TODO this clashed with the queirer and the distributor, so we cannot
	// run them in the same process.
	t.server.HTTP.PathPrefix("/api/prom").Handler(middleware.AuthenticateUser.Wrap(t.alertmanager))
	return t.alertmanager, nil
}

func (t *Cortex) initCompactor(cfg *Config) (serv services.Service, err error) {
	cfg.Compactor.ShardingRing.ListenPort = cfg.Server.GRPCListenPort
	cfg.Compactor.ShardingRing.KVStore.MemberlistKV = t.memberlistKV.GetMemberlistKV

	t.compactor, err = compactor.NewCompactor(cfg.Compactor, cfg.TSDB, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	// Expose HTTP endpoints.
	t.server.HTTP.HandleFunc("/compactor_ring", t.compactor.RingHandler)

	return t.compactor, nil
}

func (t *Cortex) initMemberlistKV(cfg *Config) (services.Service, error) {
	cfg.MemberlistKV.MetricsRegisterer = prometheus.DefaultRegisterer
	cfg.MemberlistKV.Codecs = []codec.Codec{
		ring.GetCodec(),
	}
	t.memberlistKV = memberlist.NewKVInit(&cfg.MemberlistKV)

	return services.NewIdleService(nil, func(_ error) error {
		t.memberlistKV.Stop()
		return nil
	}), nil
}

func (t *Cortex) initDataPurger(cfg *Config) (services.Service, error) {
	if !cfg.DataPurgerConfig.Enable {
		return nil, nil
	}

	var indexClient chunk.IndexClient
	indexClient, err := storage.NewIndexClient(cfg.Storage.DeleteStoreConfig.Store, cfg.Storage, cfg.Schema)
	if err != nil {
		return nil, err
	}

	deleteStore, err := chunk.NewDeleteStore(cfg.Storage.DeleteStoreConfig, indexClient)
	if err != nil {
		return nil, err
	}

	storageClient, err := storage.NewObjectClient(cfg.DataPurgerConfig.ObjectStoreType, cfg.Storage)
	if err != nil {
		return nil, err
	}

	t.dataPurger, err = purger.NewDataPurger(cfg.DataPurgerConfig, deleteStore, t.store, storageClient)
	if err != nil {
		return nil, err
	}

	var deleteRequestHandler *purger.DeleteRequestHandler
	deleteRequestHandler, err = purger.NewDeleteRequestHandler(deleteStore)
	if err != nil {
		return nil, err
	}

	adminRouter := t.server.HTTP.PathPrefix(cfg.HTTPPrefix + "/api/v1/admin/tsdb").Subrouter()

	adminRouter.Path("/delete_series").Methods("PUT", "POST").Handler(t.httpAuthMiddleware.Wrap(http.HandlerFunc(deleteRequestHandler.AddDeleteRequestHandler)))
	adminRouter.Path("/delete_series").Methods("GET").Handler(t.httpAuthMiddleware.Wrap(http.HandlerFunc(deleteRequestHandler.GetAllDeleteRequestsHandler)))

	return t.dataPurger, nil
}

type module struct {
	deps []moduleName

	// service for this module (can return nil)
	service func(t *Cortex, cfg *Config) (services.Service, error)

	// service that will be wrapped into moduleServiceWrapper, to wait for dependencies to start / end
	// (can return nil)
	wrappedService func(t *Cortex, cfg *Config) (services.Service, error)
}

var modules = map[moduleName]module{
	Server: {
		// we cannot use 'wrappedService', as stopped Server service is currently a signal to Cortex
		// that it should shutdown. If we used wrappedService, it wouldn't stop until
		// all services that depend on it stopped first... but there is nothing that would make them stop.
		service: (*Cortex).initServer,
	},

	RuntimeConfig: {
		wrappedService: (*Cortex).initRuntimeConfig,
	},

	MemberlistKV: {
		wrappedService: (*Cortex).initMemberlistKV,
	},

	Ring: {
		deps:           []moduleName{Server, RuntimeConfig, MemberlistKV},
		wrappedService: (*Cortex).initRing,
	},

	Overrides: {
		deps:           []moduleName{RuntimeConfig},
		wrappedService: (*Cortex).initOverrides,
	},

	Distributor: {
		deps:           []moduleName{Ring, Server, Overrides},
		wrappedService: (*Cortex).initDistributor,
	},

	Store: {
		deps:           []moduleName{Overrides},
		wrappedService: (*Cortex).initStore,
	},

	Ingester: {
		deps:           []moduleName{Overrides, Store, Server, RuntimeConfig, MemberlistKV},
		wrappedService: (*Cortex).initIngester,
	},

	Querier: {
		deps:           []moduleName{Distributor, Store, Ring, Server, StoreQueryable},
		wrappedService: (*Cortex).initQuerier,
	},

	StoreQueryable: {
		deps:           []moduleName{Store},
		wrappedService: (*Cortex).initStoreQueryable,
	},

	QueryFrontend: {
		deps:           []moduleName{Server, Overrides},
		wrappedService: (*Cortex).initQueryFrontend,
	},

	TableManager: {
		deps:           []moduleName{Server},
		wrappedService: (*Cortex).initTableManager,
	},

	Ruler: {
		deps:           []moduleName{Distributor, Store, StoreQueryable},
		wrappedService: (*Cortex).initRuler,
	},

	Configs: {
		deps:           []moduleName{Server},
		wrappedService: (*Cortex).initConfig,
	},

	AlertManager: {
		deps:           []moduleName{Server},
		wrappedService: (*Cortex).initAlertManager,
	},

	Compactor: {
		deps:           []moduleName{Server},
		wrappedService: (*Cortex).initCompactor,
	},

	DataPurger: {
		deps:           []moduleName{Store, Server},
		wrappedService: (*Cortex).initDataPurger,
	},

	All: {
		deps: []moduleName{Querier, Ingester, Distributor, TableManager},
	},
}
