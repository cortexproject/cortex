package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func main() {
	var (
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		logConfig        server.Config
		querierConfig    querier.Config
		limits           validation.Limits
	)
	flagext.RegisterFlags(&chunkStoreConfig, &schemaConfig, &storageConfig, &logConfig, &querierConfig, &limits)
	flag.Parse()

	util.InitLogger(&logConfig)

	// 3 levels of stuff to initialize before we can get started
	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		level.Error(util.Logger).Log("failed to set up overrides", err)
		os.Exit(1)
	}

	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides, nil, nil)
	if err != nil {
		level.Error(util.Logger).Log("failed to set up chunk store", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	storeQueryable := querier.NewChunkStoreQueryable(querierConfig, chunkStore)
	_, engine := querier.New(querierConfig, noopQuerier{}, storeQueryable, nil, nil)

	if flag.NArg() != 1 {
		level.Error(util.Logger).Log("usage: oneshot <options> promql-query")
		os.Exit(1)
	}

	// Now execute the query
	query, err := engine.NewInstantQuery(storeQueryable, flag.Arg(0), time.Now().Add(-time.Hour*24))
	if err != nil {
		level.Error(util.Logger).Log("error in query:", err)
		os.Exit(1)
	}
	ctx := user.InjectOrgID(context.Background(), "2")

	result := query.Exec(ctx)

	fmt.Printf("result: error %v %s\n", result.Err, result.Value)
}

// Stub out distributor because we only want to query the store
type noopQuerier struct{}

func (n noopQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, nil
}

func (n noopQuerier) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return nil, nil
}

func (n noopQuerier) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, nil
}

func (n noopQuerier) LabelNames(context.Context) ([]string, error) {
	return nil, nil
}

func (n noopQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (n noopQuerier) MetricsMetadata(context.Context) ([]scrape.MetricMetadata, error) {
	return nil, nil
}
