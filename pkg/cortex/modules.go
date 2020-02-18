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
	v1 "github.com/prometheus/prometheus/web/api/v1"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/chunk"
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
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
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
	QuerierChunkStore
	QueryFrontend
	Store
	TableManager
	Ruler
	Configs
	AlertManager
	Compactor
	MemberlistKV
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
	case QuerierChunkStore:
		return "querier-chunk-store"
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
	case "querier-chunk-store":
		*m = QuerierChunkStore
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

func (t *Cortex) initServer(cfg *Config) (err error) {
	t.server, err = server.New(cfg.Server)
	return
}

func (t *Cortex) stopServer() (err error) {
	t.server.Shutdown()
	return
}

func (t *Cortex) initRing(cfg *Config) (err error) {
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.runtimeConfig)
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.memberlistKVState.getMemberlistKV
	t.ring, err = ring.New(cfg.Ingester.LifecyclerConfig.RingConfig, "ingester", ring.IngesterRingKey)
	if err != nil {
		return
	}
	prometheus.MustRegister(t.ring)
	t.server.HTTP.Handle("/ring", t.ring)
	return
}

func (t *Cortex) initRuntimeConfig(cfg *Config) (err error) {
	if cfg.RuntimeConfig.LoadPath == "" {
		cfg.RuntimeConfig.LoadPath = cfg.LimitsConfig.PerTenantOverrideConfig
		cfg.RuntimeConfig.ReloadPeriod = cfg.LimitsConfig.PerTenantOverridePeriod
	}
	cfg.RuntimeConfig.Loader = loadRuntimeConfig

	// make sure to set default limits before we start loading configuration into memory
	validation.SetDefaultLimitsForYAMLUnmarshalling(cfg.LimitsConfig)

	t.runtimeConfig, err = runtimeconfig.NewRuntimeConfigManager(cfg.RuntimeConfig, prometheus.DefaultRegisterer)
	return err
}

func (t *Cortex) stopRuntimeConfig() (err error) {
	t.runtimeConfig.Stop()
	return nil
}

func (t *Cortex) initOverrides(cfg *Config) (err error) {
	t.overrides, err = validation.NewOverrides(cfg.LimitsConfig, tenantLimitsFromRuntimeConfig(t.runtimeConfig))
	return err
}

func (t *Cortex) initDistributor(cfg *Config) (err error) {
	cfg.Distributor.DistributorRing.ListenPort = cfg.Server.GRPCListenPort
	cfg.Distributor.DistributorRing.KVStore.MemberlistKV = t.memberlistKVState.getMemberlistKV

	// Check whether the distributor can join the distributors ring, which is
	// whenever it's not running as an internal dependency (ie. querier or
	// ruler's dependency)
	canJoinDistributorsRing := (cfg.Target == All || cfg.Target == Distributor)

	t.distributor, err = distributor.New(cfg.Distributor, cfg.IngesterClient, t.overrides, t.ring, canJoinDistributorsRing)
	if err != nil {
		return
	}

	t.server.HTTP.HandleFunc("/all_user_stats", t.distributor.AllUserStatsHandler)
	t.server.HTTP.Handle("/api/prom/push", t.httpAuthMiddleware.Wrap(http.HandlerFunc(t.distributor.PushHandler)))
	t.server.HTTP.Handle("/ha-tracker", t.distributor.Replicas)
	return
}

func (t *Cortex) stopDistributor() (err error) {
	t.distributor.Stop()
	return nil
}

func (t *Cortex) initQuerier(cfg *Config) (err error) {
	queryable, engine := querier.New(cfg.Querier, t.distributor, querier.NewChunkStoreQueryable(cfg.Querier, t.querierChunkStore))
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
	subrouter.PathPrefix("/api/v1").Handler(t.httpAuthMiddleware.Wrap(promRouter))
	subrouter.Path("/read").Handler(t.httpAuthMiddleware.Wrap(querier.RemoteReadHandler(queryable)))
	subrouter.Path("/validate_expr").Handler(t.httpAuthMiddleware.Wrap(http.HandlerFunc(t.distributor.ValidateExprHandler)))
	subrouter.Path("/chunks").Handler(t.httpAuthMiddleware.Wrap(querier.ChunksHandler(queryable)))
	subrouter.Path("/user_stats").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(t.distributor.UserStatsHandler)))

	// Start the query frontend worker once the query engine and the store
	// have been successfully initialized.
	t.worker, err = frontend.NewWorker(cfg.Worker, httpgrpc_server.NewServer(t.server.HTTPServer.Handler), util.Logger)
	if err != nil {
		return
	}

	// Once the execution reaches this point, all synchronous initialization has been
	// done and the querier is ready to serve queries, so we're just returning a 202.
	t.server.HTTP.Path("/ready").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	return
}

func (t *Cortex) stopQuerier() error {
	t.worker.Stop()
	return nil
}

func (t *Cortex) initQuerierChunkStore(cfg *Config) error {
	if cfg.Storage.Engine == storage.StorageEngineChunks {
		t.querierChunkStore = t.store
		return nil
	}

	if cfg.Storage.Engine == storage.StorageEngineTSDB {
		store, err := querier.NewBlockQuerier(cfg.TSDB, cfg.Server.LogLevel, prometheus.DefaultRegisterer)
		if err != nil {
			return err
		}

		t.querierChunkStore = store
		return nil
	}

	return fmt.Errorf("unknown storage engine '%s'", cfg.Storage.Engine)
}

func (t *Cortex) stopQuerierChunkStore() error {
	return nil
}

func (t *Cortex) initIngester(cfg *Config) (err error) {
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.runtimeConfig)
	cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.memberlistKVState.getMemberlistKV
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
	t.server.HTTP.Path("/ready").Handler(http.HandlerFunc(t.ingester.ReadinessHandler))
	t.server.HTTP.Path("/flush").Handler(http.HandlerFunc(t.ingester.FlushHandler))
	t.server.HTTP.Path("/shutdown").Handler(http.HandlerFunc(t.ingester.ShutdownHandler))
	return
}

func (t *Cortex) stopIngester() error {
	t.ingester.Shutdown()
	return nil
}

func (t *Cortex) initStore(cfg *Config) (err error) {
	if cfg.Storage.Engine == storage.StorageEngineTSDB {
		return nil
	}
	err = cfg.Schema.Load()
	if err != nil {
		return
	}

	t.store, err = storage.NewStore(cfg.Storage, cfg.ChunkStore, cfg.Schema, t.overrides)
	return
}

func (t *Cortex) stopStore() error {
	if t.store != nil {
		t.store.Stop()
	}
	return nil
}

func (t *Cortex) initQueryFrontend(cfg *Config) (err error) {
	t.frontend, err = frontend.New(cfg.Frontend, util.Logger)
	if err != nil {
		return
	}
	tripperware, cache, err := queryrange.NewTripperware(cfg.QueryRange, util.Logger, t.overrides, queryrange.PrometheusCodec, queryrange.PrometheusResponseExtractor)
	if err != nil {
		return err
	}
	t.cache = cache
	t.frontend.Wrap(tripperware)

	frontend.RegisterFrontendServer(t.server.GRPC, t.frontend)
	t.server.HTTP.PathPrefix(cfg.HTTPPrefix).Handler(
		t.httpAuthMiddleware.Wrap(
			t.frontend.Handler(),
		),
	)
	return
}

func (t *Cortex) stopQueryFrontend() (err error) {
	t.frontend.Close()
	if t.cache != nil {
		t.cache.Stop()
		t.cache = nil
	}
	return
}

func (t *Cortex) initTableManager(cfg *Config) error {
	if cfg.Storage.Engine == storage.StorageEngineTSDB {
		return nil // table manager isn't used in v2
	}

	err := cfg.Schema.Load()
	if err != nil {
		return err
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
		return err
	}

	bucketClient, err := storage.NewBucketClient(cfg.Storage)
	util.CheckFatal("initializing bucket client", err)

	t.tableManager, err = chunk.NewTableManager(cfg.TableManager, cfg.Schema, cfg.Ingester.MaxChunkAge, tableClient, bucketClient)
	if err != nil {
		return err
	}
	t.tableManager.Start()
	return nil
}

func (t *Cortex) stopTableManager() error {
	if t.tableManager != nil {
		t.tableManager.Stop()
	}

	return nil
}

func (t *Cortex) initRuler(cfg *Config) (err error) {
	cfg.Ruler.Ring.ListenPort = cfg.Server.GRPCListenPort
	cfg.Ruler.Ring.KVStore.MemberlistKV = t.memberlistKVState.getMemberlistKV
	queryable, engine := querier.New(cfg.Querier, t.distributor, querier.NewChunkStoreQueryable(cfg.Querier, t.querierChunkStore))

	t.ruler, err = ruler.NewRuler(cfg.Ruler, engine, queryable, t.distributor, prometheus.DefaultRegisterer, util.Logger)
	if err != nil {
		return err
	}

	if cfg.Ruler.EnableAPI {
		subrouter := t.server.HTTP.PathPrefix(cfg.HTTPPrefix).Subrouter()
		t.ruler.RegisterRoutes(subrouter)
		ruler.RegisterRulerServer(t.server.GRPC, t.ruler)
	}

	t.server.HTTP.Handle("/ruler_ring", t.ruler)
	return
}

func (t *Cortex) stopRuler() error {
	t.ruler.Stop()
	return nil
}

func (t *Cortex) initConfigs(cfg *Config) (err error) {
	t.configDB, err = db.New(cfg.ConfigDB)
	if err != nil {
		return
	}

	t.configAPI = api.New(t.configDB)
	t.configAPI.RegisterRoutes(t.server.HTTP)
	return
}

func (t *Cortex) stopConfigs() error {
	t.configDB.Close()
	return nil
}

func (t *Cortex) initAlertmanager(cfg *Config) (err error) {
	t.alertmanager, err = alertmanager.NewMultitenantAlertmanager(&cfg.Alertmanager, util.Logger)
	if err != nil {
		return err
	}
	go t.alertmanager.Run()

	t.server.HTTP.PathPrefix("/status").Handler(t.alertmanager.GetStatusHandler())

	// TODO this clashed with the queirer and the distributor, so we cannot
	// run them in the same process.
	t.server.HTTP.PathPrefix("/api/prom").Handler(middleware.AuthenticateUser.Wrap(t.alertmanager))
	return
}

func (t *Cortex) stopAlertmanager() error {
	t.alertmanager.Stop()
	return nil
}

func (t *Cortex) initCompactor(cfg *Config) (err error) {
	cfg.Compactor.ShardingRing.ListenPort = cfg.Server.GRPCListenPort
	cfg.Compactor.ShardingRing.KVStore.MemberlistKV = t.memberlistKVState.getMemberlistKV

	t.compactor, err = compactor.NewCompactor(cfg.Compactor, cfg.TSDB, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}

	t.compactor.Start()

	// Expose HTTP endpoints.
	t.server.HTTP.HandleFunc("/compactor_ring", t.compactor.RingHandler)

	return nil
}

func (t *Cortex) stopCompactor() error {
	t.compactor.Stop()
	return nil
}

func (t *Cortex) initMemberlistKV(cfg *Config) (err error) {
	cfg.MemberlistKV.MetricsRegisterer = prometheus.DefaultRegisterer
	cfg.MemberlistKV.Codecs = []codec.Codec{
		ring.GetCodec(),
	}
	t.memberlistKVState = newMemberlistKVState(&cfg.MemberlistKV)
	return nil
}

func (t *Cortex) stopMemberlistKV() (err error) {
	kv := t.memberlistKVState.kv
	if kv != nil {
		kv.Stop()
	}
	return nil
}

type module struct {
	deps []moduleName
	init func(t *Cortex, cfg *Config) error
	stop func(t *Cortex) error
}

var modules = map[moduleName]module{
	Server: {
		init: (*Cortex).initServer,
		stop: (*Cortex).stopServer,
	},

	RuntimeConfig: {
		init: (*Cortex).initRuntimeConfig,
		stop: (*Cortex).stopRuntimeConfig,
	},

	MemberlistKV: {
		init: (*Cortex).initMemberlistKV,
		stop: (*Cortex).stopMemberlistKV,
	},

	Ring: {
		deps: []moduleName{Server, RuntimeConfig, MemberlistKV},
		init: (*Cortex).initRing,
	},

	Overrides: {
		deps: []moduleName{RuntimeConfig},
		init: (*Cortex).initOverrides,
	},

	Distributor: {
		deps: []moduleName{Ring, Server, Overrides},
		init: (*Cortex).initDistributor,
		stop: (*Cortex).stopDistributor,
	},

	Store: {
		deps: []moduleName{Overrides},
		init: (*Cortex).initStore,
		stop: (*Cortex).stopStore,
	},

	Ingester: {
		deps: []moduleName{Overrides, Store, Server, RuntimeConfig, MemberlistKV},
		init: (*Cortex).initIngester,
		stop: (*Cortex).stopIngester,
	},

	Querier: {
		deps: []moduleName{Distributor, Store, Ring, Server, QuerierChunkStore},
		init: (*Cortex).initQuerier,
		stop: (*Cortex).stopQuerier,
	},

	QuerierChunkStore: {
		deps: []moduleName{Store},
		init: (*Cortex).initQuerierChunkStore,
		stop: (*Cortex).stopQuerierChunkStore,
	},

	QueryFrontend: {
		deps: []moduleName{Server, Overrides},
		init: (*Cortex).initQueryFrontend,
		stop: (*Cortex).stopQueryFrontend,
	},

	TableManager: {
		deps: []moduleName{Server},
		init: (*Cortex).initTableManager,
		stop: (*Cortex).stopTableManager,
	},

	Ruler: {
		deps: []moduleName{Distributor, Store, QuerierChunkStore},
		init: (*Cortex).initRuler,
		stop: (*Cortex).stopRuler,
	},

	Configs: {
		deps: []moduleName{Server},
		init: (*Cortex).initConfigs,
		stop: (*Cortex).stopConfigs,
	},

	AlertManager: {
		deps: []moduleName{Server},
		init: (*Cortex).initAlertmanager,
		stop: (*Cortex).stopAlertmanager,
	},

	Compactor: {
		deps: []moduleName{Server},
		init: (*Cortex).initCompactor,
		stop: (*Cortex).stopCompactor,
	},

	All: {
		deps: []moduleName{Querier, Ingester, Distributor, TableManager},
	},
}
