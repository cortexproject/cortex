package cortex

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/api"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/compactor"
	configAPI "github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/flusher"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// ModuleName is used to describe a running module
type ModuleName string

// The various modules that make up Cortex.
const (
	API                 ModuleName = "api"
	Ring                ModuleName = "ring"
	RuntimeConfig       ModuleName = "runtime-config"
	Overrides           ModuleName = "overrides"
	Server              ModuleName = "server"
	Distributor         ModuleName = "distributor"
	Ingester            ModuleName = "ingester"
	Flusher             ModuleName = "flusher"
	Querier             ModuleName = "querier"
	StoreQueryable      ModuleName = "store-queryable"
	QueryFrontend       ModuleName = "query-frontend"
	Store               ModuleName = "store"
	DeleteRequestsStore ModuleName = "delete-requests-store"
	TableManager        ModuleName = "table-manager"
	Ruler               ModuleName = "ruler"
	Configs             ModuleName = "configs"
	AlertManager        ModuleName = "alertmanager"
	Compactor           ModuleName = "compactor"
	StoreGateway        ModuleName = "store-gateway"
	MemberlistKV        ModuleName = "memberlist-kv"
	DataPurger          ModuleName = "data-purger"
	All                 ModuleName = "all"
)

func (m ModuleName) String() string {
	return string(m)
}

func (m *ModuleName) Set(s string) error {
	l := ModuleName(strings.ToLower(s))
	if _, ok := modules[l]; !ok {
		return fmt.Errorf("unrecognised module name: %s", s)
	}
	*m = l
	return nil
}

func (m ModuleName) MarshalYAML() (interface{}, error) {
	return m.String(), nil
}

func (m *ModuleName) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return m.Set(s)
}

func (t *Cortex) initAPI() (services.Service, error) {
	t.Cfg.API.ServerPrefix = t.Cfg.Server.PathPrefix
	t.Cfg.API.LegacyHTTPPrefix = t.Cfg.HTTPPrefix

	a, err := api.New(t.Cfg.API, t.Server, util.Logger)
	if err != nil {
		return nil, err
	}

	t.API = a

	t.API.RegisterAPI(t.Cfg)

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
	t.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Ring, err = ring.New(t.Cfg.Ingester.LifecyclerConfig.RingConfig, "ingester", ring.IngesterRingKey)
	if err != nil {
		return nil, err
	}
	prometheus.MustRegister(t.Ring)

	t.API.RegisterRing(t.Ring)

	return t.Ring, nil
}

func (t *Cortex) initRuntimeConfig() (services.Service, error) {
	if t.Cfg.RuntimeConfig.LoadPath == "" {
		t.Cfg.RuntimeConfig.LoadPath = t.Cfg.LimitsConfig.PerTenantOverrideConfig
		t.Cfg.RuntimeConfig.ReloadPeriod = t.Cfg.LimitsConfig.PerTenantOverridePeriod
	}
	t.Cfg.RuntimeConfig.Loader = loadRuntimeConfig

	// make sure to set default limits before we start loading configuration into memory
	validation.SetDefaultLimitsForYAMLUnmarshalling(t.Cfg.LimitsConfig)

	serv, err := runtimeconfig.NewRuntimeConfigManager(t.Cfg.RuntimeConfig, prometheus.DefaultRegisterer)
	t.RuntimeConfig = serv
	return serv, err
}

func (t *Cortex) initOverrides() (serv services.Service, err error) {
	t.Overrides, err = validation.NewOverrides(t.Cfg.LimitsConfig, tenantLimitsFromRuntimeConfig(t.RuntimeConfig))
	// overrides don't have operational state, nor do they need to do anything more in starting/stopping phase,
	// so there is no need to return any service.
	return nil, err
}

func (t *Cortex) initDistributor() (serv services.Service, err error) {
	t.Cfg.Distributor.DistributorRing.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Distributor.DistributorRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV

	// Check whether the distributor can join the distributors ring, which is
	// whenever it's not running as an internal dependency (ie. querier or
	// ruler's dependency)
	canJoinDistributorsRing := (t.Cfg.Target == All || t.Cfg.Target == Distributor)

	t.Distributor, err = distributor.New(t.Cfg.Distributor, t.Cfg.IngesterClient, t.Overrides, t.Ring, canJoinDistributorsRing)
	if err != nil {
		return
	}

	t.API.RegisterDistributor(t.Distributor, t.Cfg.Distributor)

	return t.Distributor, nil
}

func (t *Cortex) initQuerier() (serv services.Service, err error) {
	queryable, engine := querier.New(t.Cfg.Querier, t.Distributor, t.StoreQueryable, t.TombstonesLoader, prometheus.DefaultRegisterer)

	// if we are not configured for single binary mode then the querier needs to register its paths externally
	registerExternally := t.Cfg.Target != All
	handler := t.API.RegisterQuerier(queryable, engine, t.Distributor, registerExternally, t.TombstonesLoader)

	// single binary mode requires a properly configured worker.  if the operator did not attempt to configure the
	//  worker we will attempt an automatic configuration here
	if t.Cfg.Worker.Address == "" && t.Cfg.Target == All {
		address := fmt.Sprintf("127.0.0.1:%d", t.Cfg.Server.GRPCListenPort)
		level.Warn(util.Logger).Log("msg", "Worker address is empty in single binary mode.  Attempting automatic worker configuration.  If queries are unresponsive consider configuring the worker explicitly.", "address", address)
		t.Cfg.Worker.Address = address
	}

	// Query frontend worker will only be started after all its dependencies are started, not here.
	// Worker may also be nil, if not configured, which is OK.
	worker, err := frontend.NewWorker(t.Cfg.Worker, t.Cfg.Querier, httpgrpc_server.NewServer(handler), util.Logger)
	if err != nil {
		return
	}

	return worker, nil
}

func (t *Cortex) initStoreQueryable() (services.Service, error) {
	if t.Cfg.Storage.Engine == storage.StorageEngineChunks {
		t.StoreQueryable = querier.NewChunkStoreQueryable(t.Cfg.Querier, t.Store)
		return nil, nil
	}

	if t.Cfg.Storage.Engine == storage.StorageEngineTSDB && !t.Cfg.TSDB.StoreGatewayEnabled {
		storeQueryable, err := querier.NewBlockQueryable(t.Cfg.TSDB, t.Cfg.Server.LogLevel, prometheus.DefaultRegisterer)
		if err != nil {
			return nil, err
		}
		t.StoreQueryable = storeQueryable
		return storeQueryable, nil
	}

	if t.Cfg.Storage.Engine == storage.StorageEngineTSDB && t.Cfg.TSDB.StoreGatewayEnabled {
		// When running in single binary, if the blocks sharding is disabled and no custom
		// store-gateway address has been configured, we can set it to the running process.
		if t.Cfg.Target == All && !t.Cfg.StoreGateway.ShardingEnabled && t.Cfg.Querier.StoreGatewayAddresses == "" {
			t.Cfg.Querier.StoreGatewayAddresses = fmt.Sprintf("127.0.0.1:%d", t.Cfg.Server.GRPCListenPort)
		}

		storeQueryable, err := querier.NewBlocksStoreQueryableFromConfig(t.Cfg.Querier, t.Cfg.StoreGateway, t.Cfg.TSDB, util.Logger, prometheus.DefaultRegisterer)
		if err != nil {
			return nil, err
		}
		t.StoreQueryable = storeQueryable
		return storeQueryable, nil
	}

	return nil, fmt.Errorf("unknown storage engine '%s'", t.Cfg.Storage.Engine)
}

func (t *Cortex) initIngester() (serv services.Service, err error) {
	t.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Ingester.LifecyclerConfig.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Ingester.TSDBEnabled = t.Cfg.Storage.Engine == storage.StorageEngineTSDB
	t.Cfg.Ingester.TSDBConfig = t.Cfg.TSDB
	t.Cfg.Ingester.ShardByAllLabels = t.Cfg.Distributor.ShardByAllLabels

	t.Ingester, err = ingester.New(t.Cfg.Ingester, t.Cfg.IngesterClient, t.Overrides, t.Store, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	t.API.RegisterIngester(t.Ingester, t.Cfg.Distributor)

	return t.Ingester, nil
}

func (t *Cortex) initFlusher() (serv services.Service, err error) {
	t.Flusher, err = flusher.New(
		t.Cfg.Flusher,
		t.Cfg.Ingester,
		t.Cfg.IngesterClient,
		t.Store,
		prometheus.DefaultRegisterer,
	)
	if err != nil {
		return
	}

	return t.Flusher, nil
}

func (t *Cortex) initStore() (serv services.Service, err error) {
	if t.Cfg.Storage.Engine == storage.StorageEngineTSDB {
		return nil, nil
	}
	err = t.Cfg.Schema.Load()
	if err != nil {
		return
	}

	t.Store, err = storage.NewStore(t.Cfg.Storage, t.Cfg.ChunkStore, t.Cfg.Schema, t.Overrides, prometheus.DefaultRegisterer, t.TombstonesLoader)
	if err != nil {
		return
	}

	return services.NewIdleService(nil, func(_ error) error {
		t.Store.Stop()
		return nil
	}), nil
}

func (t *Cortex) initDeleteRequestsStore() (serv services.Service, err error) {
	if !t.Cfg.DataPurgerConfig.Enable {
		// until we need to explicitly enable delete series support we need to do create TombstonesLoader without DeleteStore which acts as noop
		t.TombstonesLoader = purger.NewTombstonesLoader(nil, nil)

		return
	}

	var indexClient chunk.IndexClient
	indexClient, err = storage.NewIndexClient(t.Cfg.Storage.DeleteStoreConfig.Store, t.Cfg.Storage, t.Cfg.Schema)
	if err != nil {
		return
	}

	t.DeletesStore, err = purger.NewDeleteStore(t.Cfg.Storage.DeleteStoreConfig, indexClient)
	if err != nil {
		return
	}

	t.TombstonesLoader = purger.NewTombstonesLoader(t.DeletesStore, prometheus.DefaultRegisterer)

	return
}

func (t *Cortex) initQueryFrontend() (serv services.Service, err error) {
	// Load the schema only if sharded queries is set.
	if t.Cfg.QueryRange.ShardedQueries {
		err = t.Cfg.Schema.Load()
		if err != nil {
			return
		}
	}

	t.Frontend, err = frontend.New(t.Cfg.Frontend, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	tripperware, cache, err := queryrange.NewTripperware(
		t.Cfg.QueryRange,
		util.Logger,
		t.Overrides,
		queryrange.PrometheusCodec,
		queryrange.PrometheusResponseExtractor{},
		t.Cfg.Schema,
		promql.EngineOpts{
			Logger:     util.Logger,
			Reg:        prometheus.DefaultRegisterer,
			MaxSamples: t.Cfg.Querier.MaxSamples,
			Timeout:    t.Cfg.Querier.Timeout,
		},
		t.Cfg.Querier.QueryIngestersWithin,
		prometheus.DefaultRegisterer,
		t.TombstonesLoader,
	)

	if err != nil {
		return nil, err
	}
	t.Cache = cache
	t.Frontend.Wrap(tripperware)

	t.API.RegisterQueryFrontend(t.Frontend)

	return services.NewIdleService(nil, func(_ error) error {
		t.Frontend.Close()
		if t.Cache != nil {
			t.Cache.Stop()
			t.Cache = nil
		}
		return nil
	}), nil
}

func (t *Cortex) initTableManager() (services.Service, error) {
	if t.Cfg.Storage.Engine == storage.StorageEngineTSDB {
		return nil, nil // table manager isn't used in v2
	}

	err := t.Cfg.Schema.Load()
	if err != nil {
		return nil, err
	}

	// Assume the newest config is the one to use
	lastConfig := &t.Cfg.Schema.Configs[len(t.Cfg.Schema.Configs)-1]

	if (t.Cfg.TableManager.ChunkTables.WriteScale.Enabled ||
		t.Cfg.TableManager.IndexTables.WriteScale.Enabled ||
		t.Cfg.TableManager.ChunkTables.InactiveWriteScale.Enabled ||
		t.Cfg.TableManager.IndexTables.InactiveWriteScale.Enabled ||
		t.Cfg.TableManager.ChunkTables.ReadScale.Enabled ||
		t.Cfg.TableManager.IndexTables.ReadScale.Enabled ||
		t.Cfg.TableManager.ChunkTables.InactiveReadScale.Enabled ||
		t.Cfg.TableManager.IndexTables.InactiveReadScale.Enabled) &&
		t.Cfg.Storage.AWSStorageConfig.Metrics.URL == "" {
		level.Error(util.Logger).Log("msg", "WriteScale is enabled but no Metrics URL has been provided")
		os.Exit(1)
	}

	tableClient, err := storage.NewTableClient(lastConfig.IndexType, t.Cfg.Storage)
	if err != nil {
		return nil, err
	}

	bucketClient, err := storage.NewBucketClient(t.Cfg.Storage)
	util.CheckFatal("initializing bucket client", err)

	t.TableManager, err = chunk.NewTableManager(t.Cfg.TableManager, t.Cfg.Schema, t.Cfg.Ingester.MaxChunkAge, tableClient, bucketClient, prometheus.DefaultRegisterer)
	return t.TableManager, err
}

func (t *Cortex) initRuler() (serv services.Service, err error) {
	t.Cfg.Ruler.Ring.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Ruler.Ring.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	queryable, engine := querier.New(t.Cfg.Querier, t.Distributor, t.StoreQueryable, t.TombstonesLoader, prometheus.DefaultRegisterer)

	t.Ruler, err = ruler.NewRuler(t.Cfg.Ruler, engine, queryable, t.Distributor, prometheus.DefaultRegisterer, util.Logger)
	if err != nil {
		return
	}

	// Expose HTTP endpoints.
	t.API.RegisterRuler(t.Ruler, t.Cfg.Ruler.EnableAPI)

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
	t.Alertmanager, err = alertmanager.NewMultitenantAlertmanager(&t.Cfg.Alertmanager, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}
	t.API.RegisterAlertmanager(t.Alertmanager, t.Cfg.Target == AlertManager)
	return t.Alertmanager, nil
}

func (t *Cortex) initCompactor() (serv services.Service, err error) {
	t.Cfg.Compactor.ShardingRing.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Compactor.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV

	t.Compactor, err = compactor.NewCompactor(t.Cfg.Compactor, t.Cfg.TSDB, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return
	}

	// Expose HTTP endpoints.
	t.API.RegisterCompactor(t.Compactor)
	return t.Compactor, nil
}

func (t *Cortex) initStoreGateway() (serv services.Service, err error) {
	if t.Cfg.Storage.Engine != storage.StorageEngineTSDB {
		return nil, nil
	}

	t.Cfg.StoreGateway.ShardingRing.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.StoreGateway.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV

	t.StoreGateway, err = storegateway.NewStoreGateway(t.Cfg.StoreGateway, t.Cfg.TSDB, t.Cfg.Server.LogLevel, util.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	// Expose HTTP endpoints.
	t.API.RegisterStoreGateway(t.StoreGateway)

	return t.StoreGateway, nil
}

func (t *Cortex) initMemberlistKV() (services.Service, error) {
	t.Cfg.MemberlistKV.MetricsRegisterer = prometheus.DefaultRegisterer
	t.Cfg.MemberlistKV.Codecs = []codec.Codec{
		ring.GetCodec(),
	}
	t.MemberlistKV = memberlist.NewKVInit(&t.Cfg.MemberlistKV)

	return services.NewIdleService(nil, func(_ error) error {
		t.MemberlistKV.Stop()
		return nil
	}), nil
}

func (t *Cortex) initDataPurger() (services.Service, error) {
	if !t.Cfg.DataPurgerConfig.Enable {
		return nil, nil
	}

	storageClient, err := storage.NewObjectClient(t.Cfg.DataPurgerConfig.ObjectStoreType, t.Cfg.Storage)
	if err != nil {
		return nil, err
	}

	t.DataPurger, err = purger.NewDataPurger(t.Cfg.DataPurgerConfig, t.DeletesStore, t.Store, storageClient, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	t.API.RegisterPurger(t.DeletesStore)

	return t.DataPurger, nil
}

type module struct {
	deps []ModuleName

	// Service that will be wrapped into moduleServiceWrapper, to wait for dependencies to start / end
	// (can return nil)
	wrappedService func(t *Cortex) (services.Service, error)
}

var modules = map[ModuleName]module{
	Server: {
		wrappedService: (*Cortex).initServer,
	},

	API: {
		deps:           []ModuleName{Server},
		wrappedService: (*Cortex).initAPI,
	},

	RuntimeConfig: {
		wrappedService: (*Cortex).initRuntimeConfig,
	},

	MemberlistKV: {
		wrappedService: (*Cortex).initMemberlistKV,
	},

	Ring: {
		deps:           []ModuleName{API, RuntimeConfig, MemberlistKV},
		wrappedService: (*Cortex).initRing,
	},

	Overrides: {
		deps:           []ModuleName{RuntimeConfig},
		wrappedService: (*Cortex).initOverrides,
	},

	Distributor: {
		deps:           []ModuleName{Ring, API, Overrides},
		wrappedService: (*Cortex).initDistributor,
	},

	Store: {
		deps:           []ModuleName{Overrides, DeleteRequestsStore},
		wrappedService: (*Cortex).initStore,
	},

	DeleteRequestsStore: {
		wrappedService: (*Cortex).initDeleteRequestsStore,
	},

	Ingester: {
		deps:           []ModuleName{Overrides, Store, API, RuntimeConfig, MemberlistKV},
		wrappedService: (*Cortex).initIngester,
	},

	Flusher: {
		deps:           []ModuleName{Store, API},
		wrappedService: (*Cortex).initFlusher,
	},

	Querier: {
		deps:           []ModuleName{Distributor, Store, Ring, API, StoreQueryable},
		wrappedService: (*Cortex).initQuerier,
	},

	StoreQueryable: {
		deps:           []ModuleName{Store},
		wrappedService: (*Cortex).initStoreQueryable,
	},

	QueryFrontend: {
		deps:           []ModuleName{API, Overrides, DeleteRequestsStore},
		wrappedService: (*Cortex).initQueryFrontend,
	},

	TableManager: {
		deps:           []ModuleName{API},
		wrappedService: (*Cortex).initTableManager,
	},

	Ruler: {
		deps:           []ModuleName{Distributor, Store, StoreQueryable},
		wrappedService: (*Cortex).initRuler,
	},

	Configs: {
		deps:           []ModuleName{API},
		wrappedService: (*Cortex).initConfig,
	},

	AlertManager: {
		deps:           []ModuleName{API},
		wrappedService: (*Cortex).initAlertManager,
	},

	Compactor: {
		deps:           []ModuleName{API},
		wrappedService: (*Cortex).initCompactor,
	},

	StoreGateway: {
		deps:           []ModuleName{API},
		wrappedService: (*Cortex).initStoreGateway,
	},

	DataPurger: {
		deps:           []ModuleName{Store, DeleteRequestsStore, API},
		wrappedService: (*Cortex).initDataPurger,
	},

	All: {
		deps: []ModuleName{QueryFrontend, Querier, Ingester, Distributor, TableManager, DataPurger, StoreGateway},
	},
}
