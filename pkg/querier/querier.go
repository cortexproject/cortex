package querier

import (
	"context"
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
	MaxConcurrent     int
	Timeout           time.Duration
	Iterators         bool
	BatchIterators    bool
	IngesterStreaming bool

	// For testing, to prevent re-registration of metrics in the promql engine.
	metricsRegisterer prometheus.Registerer
}

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
	cfg.metricsRegisterer = prometheus.DefaultRegisterer
}

// ChunkStore is the read-interface to the Chunk Store.  Made an interface here
// to reduce package coupling.
type ChunkStore interface {
	Get(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error)
}

// New builds a queryable and promql engine.
func New(cfg Config, distributor Distributor, chunkStore ChunkStore) (storage.Queryable, *promql.Engine) {
	iteratorFunc := mergeChunks
	if cfg.BatchIterators {
		iteratorFunc = batch.NewChunkMergeIterator
	} else if cfg.Iterators {
		iteratorFunc = iterators.NewChunkMergeIterator
	}

	var queryable storage.Queryable
	if cfg.IngesterStreaming {
		dq := newIngesterStreamingQueryable(distributor, iteratorFunc)
		queryable = newUnifiedChunkQueryable(chunkStore, dq, distributor, iteratorFunc)
	} else {
		cq := newChunkStoreQueryable(chunkStore, iteratorFunc)
		dq := newDistributorQueryable(distributor)
		queryable = NewQueryable(dq, cq, distributor)
	}

	engine := promql.NewEngine(util.Logger, cfg.metricsRegisterer, cfg.MaxConcurrent, cfg.Timeout)
	return queryable, engine
}

// NewQueryable creates a new Queryable for cortex.
func NewQueryable(dq, cq storage.Queryable, distributor Distributor) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		dqr, err := dq.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		cqr, err := cq.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		return querier{
			queriers:    []storage.Querier{dqr, cqr},
			distributor: distributor,
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
		}, nil
	})
}

type querier struct {
	queriers []storage.Querier

	distributor Distributor
	ctx         context.Context
	mint, maxt  int64
}

// Select implements storage.Querier.
func (q querier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	// Kludge: Prometheus passes nil SelectParams if it is doing a 'series' operation,
	// which needs only metadata.
	if sp == nil {
		return q.metadataQuery(matchers...)
	}

	sets := make(chan storage.SeriesSet, len(q.queriers))
	errs := make(chan error, len(q.queriers))
	for _, querier := range q.queriers {
		go func(querier storage.Querier) {
			set, err := querier.Select(sp, matchers...)
			if err != nil {
				errs <- err
			} else {
				sets <- set
			}
		}(querier)
	}

	result := []storage.SeriesSet{}
	for range q.queriers {
		select {
		case err := <-errs:
			return nil, err
		case set := <-sets:
			result = append(result, set)
		}
	}

	return storage.NewMergeSeriesSet(result), nil
}

// LabelsValue implements storage.Querier.
func (q querier) LabelValues(name string) ([]string, error) {
	return q.distributor.LabelValuesForLabelName(q.ctx, model.LabelName(name))
}

func (q querier) metadataQuery(matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	ms, err := q.distributor.MetricsForLabelMatchers(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, err
	}
	return metricsToSeriesSet(ms), nil
}

func (querier) Close() error {
	return nil
}
