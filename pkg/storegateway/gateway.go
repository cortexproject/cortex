package storegateway

import (
	"context"
	"flag"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Config holds the store gateway config.
type Config struct {
	ShardingEnabled bool       `yaml:"sharding_enabled"`
	ShardingRing    RingConfig `yaml:"sharding_ring"`
}

// RegisterFlags registers the Config flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ShardingRing.RegisterFlags(f)

	f.BoolVar(&cfg.ShardingEnabled, "store-gateway.sharding-enabled", false, "Shard blocks across multiple store gateway instances.")
}

// StoreGateway is the Cortex service responsible
type StoreGateway struct {
	services.Service

	gatewayCfg Config
	storageCfg cortex_tsdb.Config
	logger     log.Logger

	// Ring used for sharding blocks.
	ringLifecycler *ring.Lifecycler
	ring           *ring.Ring

	// Subservices manager (ring, lifecycler)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewStoreGateway(gatewayCfg Config, storageCfg cortex_tsdb.Config, logger log.Logger, _ prometheus.Registerer) *StoreGateway {
	g := &StoreGateway{
		gatewayCfg: gatewayCfg,
		storageCfg: storageCfg,
		logger:     logger,
	}

	g.Service = services.NewBasicService(g.starting, g.running, g.stopping)

	return g
}

func (g *StoreGateway) starting(ctx context.Context) error {
	// Initialize the store gateways ring if sharding is enabled.
	if g.gatewayCfg.ShardingEnabled {
		lifecyclerCfg := g.gatewayCfg.ShardingRing.ToLifecyclerConfig()
		lifecycler, err := ring.NewLifecycler(lifecyclerCfg, ring.NewNoopFlushTransferer(), "store-gateway", ring.StoreGatewayRingKey, false)
		if err != nil {
			return errors.Wrap(err, "unable to initialize store gateway ring lifecycler")
		}

		g.ringLifecycler = lifecycler

		ring, err := ring.New(lifecyclerCfg.RingConfig, "store-gateway", ring.StoreGatewayRingKey)
		if err != nil {
			return errors.Wrap(err, "unable to initialize store-gateway ring")
		}

		g.ring = ring

		g.subservices, err = services.NewManager(g.ringLifecycler, g.ring)
		if err == nil {
			g.subservicesWatcher = services.NewFailureWatcher()
			g.subservicesWatcher.WatchManager(g.subservices)

			err = services.StartManagerAndAwaitHealthy(ctx, g.subservices)
		}

		if err != nil {
			return errors.Wrap(err, "unable to start store-gateway dependencies")
		}
	}

	return nil
}

func (g *StoreGateway) stopping(_ error) error {
	if g.subservices != nil {
		return services.StopManagerAndAwaitStopped(context.Background(), g.subservices)
	}
	return nil
}

func (g *StoreGateway) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-g.subservicesWatcher.Chan():
			return errors.Wrap(err, "store gateway subservice failed")
		}
	}
}
