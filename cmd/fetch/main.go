package main

import (
	"flag"
	"fmt"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

/*
Simple command-line client to fetch raw data from the store
*/
func main() {
	var (
		chunkStoreConfig chunk.StoreConfig
		storageConfig    storage.Config
		schemaConfig     chunk.SchemaConfig
	)
	util.RegisterFlags(&chunkStoreConfig, &storageConfig, &schemaConfig)
	flag.Parse()

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		log.Fatalf("Error initializing storage client: %v", err)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Stop()

	// TODO: parse another time - we can't expect data in DynamoDB right up to now
	// because it is written slowly by the ingesters
	through := model.Now() - 24*60*60*1000
	from := through - 60*1000

	matcher, err := metric.NewLabelMatcher(metric.Equal, model.MetricNameLabel, "machine_cpu_cores")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "2")

	fmt.Printf("Requesting %q in interval %s-%s\n", matcher, from, through)

	iters, err := chunkStore.Get(ctx, from, through, matcher)
	if err != nil {
		log.Fatal(err)
	}
	for _, iter := range iters {
		fmt.Printf("Metric: %v\n", iter.Metric())
		fmt.Printf("Values: %v\n", iter.RangeValues(metric.Interval{OldestInclusive: from, NewestInclusive: through}))
	}
}
