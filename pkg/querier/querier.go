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
	MaxConcurrent            int
	Timeout                  time.Duration
	Iterators                bool
	BatchIterators           bool
	IngesterStreaming        bool
	MaxSamples               int
	IngesterMaxQueryLookback time.Duration

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
	f.IntVar(&cfg.MaxSamples, "querier.max-samples", 50e6, "Maximum number of samples a single query can load into memory.")
	f.DurationVar(&cfg.IngesterMaxQueryLookback, "querier.query-ingesters-within", 0, "Maximum lookback beyond which queries are not sent to ingester. 0 means all queries are sent to ingester.")
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
		queryable = newUnifiedChunkQueryable(dq, chunkStore, distributor, iteratorFunc, cfg.IngesterMaxQueryLookback)
	} else {
		cq := newChunkStoreQueryable(chunkStore, iteratorFunc)
		dq := newDistributorQueryable(distributor)
		queryable = NewQueryable(dq, cq, distributor, cfg.IngesterMaxQueryLookback)
	}

	lazyQueryable := storage.QueryableFunc(func(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
		querier, err := queryable.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		return newLazyQuerier(querier), nil
	})

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
func NewQueryable(dq, cq storage.Queryable, distributor Distributor, ingesterMaxQueryLookback time.Duration) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		cqr, err := cq.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		q := querier{
			queriers:    []storage.Querier{cqr},
			distributor: distributor,
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
		}

		// Include ingester only if maxt is within ingesterMaxQueryLookback w.r.t. current time.
		if ingesterMaxQueryLookback == 0 || maxt >= time.Now().Add(-ingesterMaxQueryLookback).UnixNano()/1e6 {
			dqr, err := dq.Querier(ctx, mint, maxt)
			if err != nil {
				return nil, err
			}
			q.queriers = append(q.queriers, dqr)
		}

		return q, nil
	})
}

type querier struct {
	queriers []storage.Querier

	distributor Distributor
	ctx         context.Context
	mint, maxt  int64
}

// Select implements storage.Querier.
func (q querier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	// Kludge: Prometheus passes nil SelectParams if it is doing a 'series' operation,
	// which needs only metadata.
	if sp == nil {
		return q.metadataQuery(matchers...)
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

	result := []storage.SeriesSet{}
	for range q.queriers {
		select {
		case err := <-errs:
			return nil, nil, err
		case set := <-sets:
			result = append(result, set)
		}
	}

	return storage.NewMergeSeriesSet(result, nil), nil, nil
}

// LabelsValue implements storage.Querier.
func (q querier) LabelValues(name string) ([]string, error) {
	return q.distributor.LabelValuesForLabelName(q.ctx, model.LabelName(name))
}

func (q querier) LabelNames() ([]string, error) {
	return nil, nil
}

func (q querier) metadataQuery(matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	ms, err := q.distributor.MetricsForLabelMatchers(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, nil, err
	}
	return metricsToSeriesSet(ms), nil, nil
}

func (querier) Close() error {
	return nil
}
