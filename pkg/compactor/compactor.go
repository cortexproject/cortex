package compactor

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// Config holds the Compactor config.
type Config struct {
}

// RegisterFlags registers the Compactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	// TODO(pracucci) build me
}

// Compactor is a multi-tenant TSDB blocks compactor based on Thanos.
type Compactor struct {
	compactorCfg Config
	storageCfg   cortex_tsdb.Config

	// Underlying compactor used to compact TSDB blocks.
	tsdbCompactor tsdb.Compactor

	// Client used to run operations on the bucket storing blocks.
	bucketClient objstore.Bucket

	// Channel used to signal when the compactor should stop.
	quit   chan struct{}
	runner sync.WaitGroup
}

// NewCompactor makes a new Compactor.
func NewCompactor(compactorCfg Config, storageCfg cortex_tsdb.Config) (*Compactor, error) {
	bucketClient, err := cortex_tsdb.NewBucketClient(context.Background(), storageCfg, "compactor", util.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	// TODO(pracucci) allow to config levels
	levels := []int64{
		int64((2 * time.Hour) / time.Millisecond),
		int64((24 * time.Hour) / time.Millisecond),
	}

	// TODO(pracucci) prometheus register
	// TODO(pracucci) should we instance our own pool?
	tsdbCompactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, util.Logger, levels, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create TSDB compactor")
	}

	c := &Compactor{
		compactorCfg:  compactorCfg,
		storageCfg:    storageCfg,
		bucketClient:  bucketClient,
		tsdbCompactor: tsdbCompactor,
	}

	// Start the compactor loop.
	c.runner.Add(1)
	go c.run()

	return c, nil
}

// Shutdown the compactor and waits until done. This may take some time
// if there's a on-going compaction.
func (c *Compactor) Shutdown() {
	close(c.quit)
	c.runner.Wait()
}

func (c *Compactor) run() {
	defer c.runner.Done()

	// TODO(pracucci) run in a loop until shutdown

	ctx := context.Background()

	c.compactUsers(ctx)
}

func (c *Compactor) compactUsers(ctx context.Context) error {
	level.Info(util.Logger).Log("msg", "discovering users from bucket")
	users, err := c.discoverUsers(ctx)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to discover users from bucket", "err", err)
		return err
	}

	for _, userID := range users {
		level.Info(util.Logger).Log("msg", "starting compaction of user blocks", "user", userID)

		if err = c.compactUser(ctx, userID); err != nil {
			level.Error(util.Logger).Log("msg", "failed to compact user blocks", "user", userID, "err", err)
			continue
		}

		level.Info(util.Logger).Log("msg", "successfully compacted user blocks", "user", userID)
	}

	return nil
}

func (c *Compactor) compactUser(ctx context.Context, userID string) error {
	bucket := cortex_tsdb.NewUserBucketClient(userID, c.bucketClient)
	fmt.Println("Bucket:", bucket)

	syncer, err := compact.NewSyncer(
		util.Logger,
		nil, // TODO(pracucci) prometheus registerer
		bucket,
		30*time.Minute, // Consistency delay
		20,             // Block sync concurrency
		false,          // Do not accept malformed indexes
		// TODO(pracucci) true,           // Enable vertical compaction
		[]*relabel.Config{})
	if err != nil {
		return errors.Wrap(err, "failed to create syncer")
	}
	fmt.Println("Syncer:", syncer)

	compactor, err := compact.NewBucketCompactor(
		util.Logger,
		syncer,
		c.tsdbCompactor,
		"/tmp/", // TODO(pracucci) compactDir
		bucket,
		1, // Compaction concurrency
	)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket compactor")
	}

	return compactor.Compact(ctx)
}

func (c *Compactor) discoverUsers(ctx context.Context) ([]string, error) {
	users := make([]string, 0)

	err := c.bucketClient.Iter(ctx, "", func(entry string) error {
		users = append(users, strings.TrimSuffix(entry, "/"))
		return nil
	})

	return users, err
}
