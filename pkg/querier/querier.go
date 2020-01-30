package querier

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
	"github.com/cortexproject/cortex/pkg/util"
)

// Config contains the configuration require to create a querier
type Config struct {
	MaxConcurrent        int
	Timeout              time.Duration
	Iterators            bool
	BatchIterators       bool
	IngesterStreaming    bool
	MaxSamples           int
	QueryIngestersWithin time.Duration `yaml:"query_ingesters_within"`

	// QueryStoreAfter the time after which queries should also be sent to the store and not just ingesters.
	QueryStoreAfter time.Duration `yaml:"query_store_after"`

	// The default evaluation interval for the promql engine.
	// Needs to be configured for subqueries to work as it is the default
	// step if not specified.
	DefaultEvaluationInterval time.Duration

	// For testing, to prevent re-registration of metrics in the promql engine.
	metricsRegisterer prometheus.Registerer `yaml:"-"`
}

var (
	errBadLookbackConfigs = errors.New("bad settings, query_store_after >= query_ingesters_within which can result in queries not being sent")
)

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxConcurrent, "querier.max-concurrent", 20, "The maximum number of concurrent queries.")
	f.DurationVar(&cfg.Timeout, "querier.timeout", 2*time.Minute, "The timeout for a query.")
	if f.Lookup("promql.lookback-delta") == nil {
		f.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}
	f.BoolVar(&cfg.Iterators, "querier.iterators", false, "Use iterators to execute query, as opposed to fully materialising the series in memory.")
	f.BoolVar(&cfg.BatchIterators, "querier.batch-iterators", false, "Use batch iterators to execute query, as opposed to fully materialising the series in memory.  Takes precedent over the -querier.iterators flag.")
	f.BoolVar(&cfg.IngesterStreaming, "querier.ingester-streaming", false, "Use streaming RPCs to query ingester.")
	f.IntVar(&cfg.MaxSamples, "querier.max-samples", 50e6, "Maximum number of samples a single query can load into memory.")
	f.DurationVar(&cfg.QueryIngestersWithin, "querier.query-ingesters-within", 0, "Maximum lookback beyond which queries are not sent to ingester. 0 means all queries are sent to ingester.")
	f.DurationVar(&cfg.DefaultEvaluationInterval, "querier.default-evaluation-interval", time.Minute, "The default evaluation interval or step size for subqueries.")
	f.DurationVar(&cfg.QueryStoreAfter, "querier.query-store-after", 0, "The time after which a metric should only be queried from storage and not just ingesters. 0 means all queries are sent to store.")
	cfg.metricsRegisterer = prometheus.DefaultRegisterer
}

// Validate the config
func (cfg *Config) Validate() error {

	// Ensure the config wont create a situation where no queriers are returned.
	if cfg.QueryIngestersWithin != 0 && cfg.QueryStoreAfter != 0 {
		if cfg.QueryStoreAfter >= cfg.QueryIngestersWithin {
			return errBadLookbackConfigs
		}
	}

	return nil
}

// ChunkStore is the read-interface to the Chunk Store.  Made an interface here
// to reduce package coupling.
type ChunkStore interface {
	Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error)
}

func getChunksIteratorFunction(cfg Config) chunkIteratorFunc {
	if cfg.BatchIterators {
		return batch.NewChunkMergeIterator
	} else if cfg.Iterators {
		return iterators.NewChunkMergeIterator
	}
	return mergeChunks
}

func NewChunkStoreQueryable(cfg Config, chunkStore ChunkStore) storage.Queryable {
	return newChunkStoreQueryable(chunkStore, getChunksIteratorFunction(cfg))
}

// New builds a queryable and promql engine.
func New(cfg Config, distributor Distributor, storeQueryable storage.Queryable) (storage.Queryable, *promql.Engine) {
	iteratorFunc := getChunksIteratorFunction(cfg)

	var queryable storage.Queryable
	dq := newDistributorQueryable(distributor, cfg.IngesterStreaming, iteratorFunc)
	queryable = NewQueryable(dq, storeQueryable, iteratorFunc, cfg)

	lazyQueryable := storage.QueryableFunc(func(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
		querier, err := queryable.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		return newLazyQuerier(querier), nil
	})

	promql.SetDefaultEvaluationInterval(cfg.DefaultEvaluationInterval)
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:        util.Logger,
		Reg:           cfg.metricsRegisterer,
		MaxConcurrent: cfg.MaxConcurrent,
		MaxSamples:    cfg.MaxSamples,
		Timeout:       cfg.Timeout,
	})
	return lazyQueryable, engine
}

// NewQueryable creates a new Queryable for cortex.
func NewQueryable(distribQuerier, storeQuerier storage.Queryable, chunkIterFn chunkIteratorFunc, cfg Config) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		q := querier{
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
			chunkIterFn: chunkIterFn,
		}

		dqr, err := distribQuerier.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		q.primary = dqr

		// Include ingester only if maxt is within queryIngestersWithin w.r.t. current time.
		now := model.Now()
		if cfg.QueryIngestersWithin == 0 || maxt >= int64(now.Add(-cfg.QueryIngestersWithin)) {
			q.queriers = append(q.queriers, dqr)
		}

		// Include store only if mint is within QueryStoreAfter w.r.t current time.
		if cfg.QueryStoreAfter == 0 || mint <= int64(now.Add(-cfg.QueryStoreAfter)) {
			cqr, err := storeQuerier.Querier(ctx, mint, maxt)
			if err != nil {
				return nil, err
			}

			q.queriers = append(q.queriers, cqr)
		}

		return q, nil
	})
}

type querier struct {
	// primary querier is used for labels and metadata queries
	primary  storage.Querier
	queriers []storage.Querier

	chunkIterFn chunkIteratorFunc
	ctx         context.Context
	mint, maxt  int64
}

// Select implements storage.Querier.
func (q querier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	// Kludge: Prometheus passes nil SelectParams if it is doing a 'series' operation,
	// which needs only metadata. Here we expect that primary querier will handle that.
	if sp == nil {
		return q.primary.Select(nil, matchers...)
	}

	if len(q.queriers) == 1 {
		return q.queriers[0].Select(sp, matchers...)
	}

	sets := make(chan storage.SeriesSet, len(q.queriers))
	errs := make(chan error, len(q.queriers))
	for _, querier := range q.queriers {
		go func(querier storage.Querier) {
			set, _, err := querier.Select(sp, matchers...)
			if err != nil {
				errs <- err
			} else {
				sets <- set
			}
		}(querier)
	}

	var result []storage.SeriesSet
	for range q.queriers {
		select {
		case err := <-errs:
			return nil, nil, err
		case set := <-sets:
			result = append(result, set)
		}
	}

	// we have all the sets from different sources (chunk from store, chunks from ingesters,
	// time series from store and time series from ingesters).
	return q.mergeSeriesSets(result), nil, nil
}

// LabelsValue implements storage.Querier.
func (q querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	return q.primary.LabelValues(name)
}

func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	return q.primary.LabelNames()
}

func (querier) Close() error {
	return nil
}

func (q querier) mergeSeriesSets(sets []storage.SeriesSet) storage.SeriesSet {
	// Here we deal with sets that are based on chunks and build single set from them.
	// Remaining sets are merged with chunks-based one using storage.NewMergeSeriesSet

	otherSets := []storage.SeriesSet(nil)
	chunks := []chunk.Chunk(nil)

	for _, set := range sets {
		if !set.Next() {
			// nothing in this set
			continue
		}

		s := set.At()
		if sc, ok := s.(SeriesWithChunks); ok {
			chunks = append(chunks, sc.Chunks()...)

			// iterate over remaining series in this set, and store chunks
			// Here we assume that all remaining series in the set are also backed-up by chunks.
			// If not, there will be panics.
			for set.Next() {
				s = set.At()
				chunks = append(chunks, s.(SeriesWithChunks).Chunks()...)
			}
		} else {
			otherSets = append(otherSets, &ignoreFirstNextSeriesSet{set: set})
		}
	}

	if len(chunks) == 0 {
		return storage.NewMergeSeriesSet(otherSets, nil)
	}

	// partitionChunks returns set with sorted series, so it can be used by NewMergeSeriesSet
	chunksSet := partitionChunks(chunks, q.mint, q.maxt, q.chunkIterFn)

	if len(otherSets) == 0 {
		return chunksSet
	}

	otherSets = append(otherSets, chunksSet)
	return storage.NewMergeSeriesSet(otherSets, nil)
}

// This series set ignores first 'Next' call and simply returns true.
type ignoreFirstNextSeriesSet struct {
	firstNextCalled bool
	set             storage.SeriesSet
}

func (pss *ignoreFirstNextSeriesSet) Next() bool {
	if pss.firstNextCalled {
		return pss.set.Next()
	}
	pss.firstNextCalled = true
	return true
}

func (pss *ignoreFirstNextSeriesSet) At() storage.Series {
	return pss.set.At()
}

func (pss *ignoreFirstNextSeriesSet) Err() error {
	return pss.set.Err()
}
