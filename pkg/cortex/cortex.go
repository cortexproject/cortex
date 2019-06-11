package cortex

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/configs/api"
	config_client "github.com/cortexproject/cortex/pkg/configs/client"
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

// The design pattern for Cortex is a series of config objects, which are
// registered for command line flags, and then a series of components that
// are instantiated and composed.  Some rules of thumb:
// - Config types should only contain 'simple' types (ints, strings, urls etc).
// - Flag validation should be done by the flag; use a flag.Value where
//   appropriate.
// - Config types should map 1:1 with a component type.
// - Config types should define flags with a common prefix.
// - It's fine to nest configs within configs, but this should match the
//   nesting of components within components.
// - Limit as much is possible sharing of configuration between config types.
//   Where necessary, use a pointer for this - avoid repetition.
// - Where a nesting of components its not obvious, it's fine to pass
//   references to other components constructors to compose them.
// - First argument for a components constructor should be its matching config
//   object.

// Config is the root config for Cortex.
type Config struct {
	Target      moduleName `yaml:"target,omitempty"`
	AuthEnabled bool       `yaml:"auth_enabled,omitempty"`
	PrintConfig bool       `yaml:"-"`

	Server         server.Config            `yaml:"server,omitempty"`
	Distributor    distributor.Config       `yaml:"distributor,omitempty"`
	Querier        querier.Config           `yaml:"querier,omitempty"`
	IngesterClient client.Config            `yaml:"ingester_client,omitempty"`
	Ingester       ingester.Config          `yaml:"ingester,omitempty"`
	Storage        storage.Config           `yaml:"storage,omitempty"`
	ChunkStore     chunk.StoreConfig        `yaml:"chunk_store,omitempty"`
	Schema         chunk.SchemaConfig       `yaml:"schema,omitempty"`
	LimitsConfig   validation.Limits        `yaml:"limits,omitempty"`
	Prealloc       client.PreallocConfig    `yaml:"prealloc,omitempty"`
	Worker         frontend.WorkerConfig    `yaml:"frontend_worker,omitempty"`
	Frontend       frontend.Config          `yaml:"frontend,omitempty"`
	TableManager   chunk.TableManagerConfig `yaml:"table_manager,omitempty"`
	Encoding       encoding.Config          `yaml:"-"` // No yaml for this, it only works with flags.

	Ruler        ruler.Config                               `yaml:"ruler,omitempty"`
	ConfigStore  config_client.Config                       `yaml:"config_store,omitempty"`
	Alertmanager alertmanager.MultitenantAlertmanagerConfig `yaml:"alertmanager,omitempty"`
}

// RegisterFlags registers flag.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	c.Server.MetricsNamespace = "cortex"
	c.Target = All
	c.Server.ExcludeRequestInLog = true
	f.Var(&c.Target, "target", "target module (default All)")
	f.BoolVar(&c.AuthEnabled, "auth.enabled", true, "Set to false to disable auth.")
	f.BoolVar(&c.PrintConfig, "print.config", false, "Print the config and exit.")

	c.Server.RegisterFlags(f)
	c.Distributor.RegisterFlags(f)
	c.Querier.RegisterFlags(f)
	c.IngesterClient.RegisterFlags(f)
	c.Ingester.RegisterFlags(f)
	c.Storage.RegisterFlags(f)
	c.ChunkStore.RegisterFlags(f)
	c.Schema.RegisterFlags(f)
	c.LimitsConfig.RegisterFlags(f)
	c.Prealloc.RegisterFlags(f)
	c.Worker.RegisterFlags(f)
	c.Frontend.RegisterFlags(f)
	c.TableManager.RegisterFlags(f)
	c.Encoding.RegisterFlags(f)

	c.Ruler.RegisterFlags(f)
	c.ConfigStore.RegisterFlags(f)
	c.Alertmanager.RegisterFlags(f)

	// These don't seem to have a home.
	flag.IntVar(&chunk_util.QueryParallelism, "querier.query-parallelism", 100, "Max subqueries run in parallel per higher-level query.")
}

// Cortex is the root datastructure for Cortex.
type Cortex struct {
	target             moduleName
	httpAuthMiddleware middleware.Interface

	server       *server.Server
	ring         *ring.Ring
	overrides    *validation.Overrides
	distributor  *distributor.Distributor
	ingester     *ingester.Ingester
	store        chunk.Store
	worker       frontend.Worker
	frontend     *frontend.Frontend
	tableManager *chunk.TableManager

	ruler        *ruler.Ruler
	configAPI    *api.API
	configDB     db.DB
	alertmanager *alertmanager.MultitenantAlertmanager
}

// New makes a new Cortex.
func New(cfg Config) (*Cortex, error) {
	if cfg.PrintConfig {
		if err := yaml.NewEncoder(os.Stdout).Encode(&cfg); err != nil {
			fmt.Println("Error encoding config:", err)
		}
		os.Exit(0)
	}

	cortex := &Cortex{
		target: cfg.Target,
	}

	cortex.setupAuthMiddleware(&cfg)

	if err := cortex.init(&cfg, cfg.Target); err != nil {
		return nil, err
	}

	return cortex, nil
}

func (t *Cortex) setupAuthMiddleware(cfg *Config) {
	if cfg.AuthEnabled {
		cfg.Server.GRPCMiddleware = []grpc.UnaryServerInterceptor{
			middleware.ServerUserHeaderInterceptor,
		}
		cfg.Server.GRPCStreamMiddleware = []grpc.StreamServerInterceptor{
			func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				switch info.FullMethod {
				// Don't check auth header on TransferChunks, as we weren't originally
				// sending it and this could cause transfers to fail on update.
				//
				// Also don't check auth /frontend.Frontend/Process, as this handles
				// queries for multiple users.
				case "/cortex.Ingester/TransferChunks", "/frontend.Frontend/Process":
					return handler(srv, ss)
				default:
					return middleware.StreamServerUserHeaderInterceptor(srv, ss, info, handler)
				}
			},
		}
		t.httpAuthMiddleware = middleware.AuthenticateUser
	} else {
		cfg.Server.GRPCMiddleware = []grpc.UnaryServerInterceptor{
			fakeGRPCAuthUniaryMiddleware,
		}
		cfg.Server.GRPCStreamMiddleware = []grpc.StreamServerInterceptor{
			fakeGRPCAuthStreamMiddleware,
		}
		t.httpAuthMiddleware = fakeHTTPAuthMiddleware
	}
}

func (t *Cortex) init(cfg *Config, m moduleName) error {
	// initialize all of our dependencies first
	for _, dep := range orderedDeps(m) {
		if err := t.initModule(cfg, dep); err != nil {
			return err
		}
	}
	// lastly, initialize the requested module
	return t.initModule(cfg, m)
}

func (t *Cortex) initModule(cfg *Config, m moduleName) error {
	level.Info(util.Logger).Log("msg", "initialising", "module", m)
	if modules[m].init != nil {
		if err := modules[m].init(t, cfg); err != nil {
			return errors.Wrap(err, fmt.Sprintf("error initialising module: %s", m))
		}
	}
	return nil
}

// Run starts Cortex running, and blocks until a signal is received.
func (t *Cortex) Run() error {
	return t.server.Run()
}

// Stop gracefully stops a Cortex.
func (t *Cortex) Stop() error {
	t.server.Shutdown()
	t.stop(t.target)
	return nil
}

func (t *Cortex) stop(m moduleName) {
	t.stopModule(m)
	deps := orderedDeps(m)
	// iterate over our deps in reverse order and call stopModule
	for i := len(deps) - 1; i >= 0; i-- {
		t.stopModule(deps[i])
	}
}

func (t *Cortex) stopModule(m moduleName) {
	level.Info(util.Logger).Log("msg", "stopping", "module", m)
	if modules[m].stop != nil {
		if err := modules[m].stop(t); err != nil {
			level.Error(util.Logger).Log("msg", "error stopping", "module", m, "err", err)
		}
	}
}

// listDeps recursively gets a list of dependencies for a passed moduleName
func listDeps(m moduleName) []moduleName {
	deps := modules[m].deps
	for _, d := range modules[m].deps {
		deps = append(deps, listDeps(d)...)
	}
	return deps
}

// orderedDeps gets a list of all dependencies ordered so that items are always after any of their dependencies.
func orderedDeps(m moduleName) []moduleName {
	deps := listDeps(m)

	// get a unique list of moduleNames, with a flag for whether they have been added to our result
	uniq := map[moduleName]bool{}
	for _, dep := range deps {
		uniq[dep] = false
	}

	result := make([]moduleName, 0, len(uniq))

	// keep looping through all modules until they have all been added to the result.

	for len(result) < len(uniq) {
	OUTER:
		for name, added := range uniq {
			if added {
				continue
			}
			for _, dep := range modules[name].deps {
				// stop processing this module if one of its dependencies has
				// not been added to the result yet.
				if !uniq[dep] {
					continue OUTER
				}
			}

			// if all of the module's dependencies have been added to the result slice,
			// then we can safely add this module to the result slice as well.
			uniq[name] = true
			result = append(result, name)
		}
	}
	return result
}
