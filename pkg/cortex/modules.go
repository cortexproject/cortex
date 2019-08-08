package cortex

import (
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
	"github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type moduleName int

// The various modules that make up Cortex.
const (
	Ring moduleName = iota
	Overrides
	Server
	Distributor
	Ingester
	Querier
	QueryFrontend
	Store
	TableManager
	Ruler
	Configs
	AlertManager
	All
)

func (m moduleName) String() string {
	switch m {
	case Ring:
		return "ring"
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
	case "all":
		*m = All
		return nil
	default:
		return fmt.Errorf("unrecognised module name: %s", s)
	}
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
	t.ring, err = ring.New(cfg.Ingester.LifecyclerConfig.RingConfig, "ingester")
	if err != nil {
		return
	}
	prometheus.MustRegister(t.ring)
	t.server.HTTP.Handle("/ring", t.ring)
	return
}

func (t *Cortex) initOverrides(cfg *Config) (err error) {
	t.overrides, err = validation.NewOverrides(cfg.LimitsConfig)
	return err
}

func (t *Cortex) stopOverrides() error {
	t.overrides.Stop()
	return nil
}

func (t *Cortex) initDistributor(cfg *Config) (err error) {
	t.distributor, err = distributor.New(cfg.Distributor, cfg.IngesterClient, t.overrides, t.ring)
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
	t.worker, err = frontend.NewWorker(cfg.Worker, httpgrpc_server.NewServer(t.server.HTTPServer.Handler), util.Logger)
	if err != nil {
		return
	}

	queryable, engine := querier.New(cfg.Querier, t.distributor, t.store)
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
		0, 0, // Remote read samples and concurrency limit.
		regexp.MustCompile(".*"),
	)
	promRouter := route.New().WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	subrouter := t.server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(t.httpAuthMiddleware.Wrap(promRouter))
	subrouter.Path("/read").Handler(t.httpAuthMiddleware.Wrap(querier.RemoteReadHandler(queryable)))
	subrouter.Path("/validate_expr").Handler(t.httpAuthMiddleware.Wrap(http.HandlerFunc(t.distributor.ValidateExprHandler)))
	subrouter.Path("/chunks").Handler(t.httpAuthMiddleware.Wrap(querier.ChunksHandler(queryable)))
	subrouter.Path("/user_stats").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(t.distributor.UserStatsHandler)))
	return
}

func (t *Cortex) stopQuerier() error {
	t.worker.Stop()
	return nil
}

func (t *Cortex) initIngester(cfg *Config) (err error) {
	cfg.Ingester.LifecyclerConfig.ListenPort = &cfg.Server.GRPCListenPort
	t.ingester, err = ingester.New(cfg.Ingester, cfg.IngesterClient, t.overrides, t.store, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	client.RegisterIngesterServer(t.server.GRPC, t.ingester)
	grpc_health_v1.RegisterHealthServer(t.server.GRPC, t.ingester)
	t.server.HTTP.Path("/ready").Handler(http.HandlerFunc(t.ingester.ReadinessHandler))
	t.server.HTTP.Path("/flush").Handler(http.HandlerFunc(t.ingester.FlushHandler))
	return
}

func (t *Cortex) stopIngester() error {
	t.ingester.Shutdown()
	return nil
}

func (t *Cortex) initStore(cfg *Config) (err error) {
	err = cfg.Schema.Load()
	if err != nil {
		return
	}

	t.store, err = storage.NewStore(cfg.Storage, cfg.ChunkStore, cfg.Schema, t.overrides)
	return
}

func (t *Cortex) stopStore() error {
	t.store.Stop()
	return nil
}

func (t *Cortex) initQueryFrontend(cfg *Config) (err error) {
	t.frontend, err = frontend.New(cfg.Frontend, util.Logger, t.overrides)
	if err != nil {
		return
	}

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
	return
}

func (t *Cortex) initTableManager(cfg *Config) error {
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
	t.tableManager.Stop()
	return nil
}

func (t *Cortex) initRuler(cfg *Config) (err error) {
	cfg.Ruler.LifecyclerConfig.ListenPort = &cfg.Server.GRPCListenPort
	queryable, engine := querier.New(cfg.Querier, t.distributor, t.store)

	t.ruler, err = ruler.NewRuler(cfg.Ruler, engine, queryable, t.distributor)
	if err != nil {
		return
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
	t.alertmanager, err = alertmanager.NewMultitenantAlertmanager(&cfg.Alertmanager, cfg.ConfigStore)
	if err != nil {
		return
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

	Ring: {
		deps: []moduleName{Server},
		init: (*Cortex).initRing,
	},

	Overrides: {
		init: (*Cortex).initOverrides,
		stop: (*Cortex).stopOverrides,
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
		deps: []moduleName{Overrides, Store, Server},
		init: (*Cortex).initIngester,
		stop: (*Cortex).stopIngester,
	},

	Querier: {
		deps: []moduleName{Distributor, Store, Ring, Server},
		init: (*Cortex).initQuerier,
		stop: (*Cortex).stopQuerier,
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
		deps: []moduleName{Distributor, Store},
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

	All: {
		deps: []moduleName{Querier, Ingester, Distributor, TableManager},
	},
}
