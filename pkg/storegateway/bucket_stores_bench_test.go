package storegateway

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func BenchmarkThanosBucketStores_SeriesBatch(b *testing.B) {
	seriesNum := []int{100, 1000, 10000, 100000}
	samplePerSeries := 100
	batchSizes := []int{1, 10, 100, 1000, 10000}

	for _, series := range seriesNum {
		b.Run(fmt.Sprintf("series_%d", series), func(b *testing.B) {
			tmpDir := b.TempDir()
			storageDir := filepath.Join(tmpDir, "storage")
			userID := "user-1"

			// generate block for benchmark
			generateBenchmarkBlock(b, storageDir, userID, series, samplePerSeries)

			// Initialize the BucketStore
			cfg := prepareStorageConfig(b)
			cfg.BucketStore.SyncDir = filepath.Join(tmpDir, "sync")

			bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
			require.NoError(b, err)

			reg := prometheus.NewPedanticRegistry()
			stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bucketClient), defaultLimitsOverrides(nil), mockLoggingLevel(), log.NewNopLogger(), reg)
			require.NoError(b, err)

			// Perform Initial Sync to load blocks
			require.NoError(b, stores.InitialSync(context.Background()))

			// Start gRPC Server
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(b, err)

			gRPCServer := grpc.NewServer()
			storepb.RegisterStoreServer(gRPCServer, stores)

			// start gRPC server
			go func() {
				if err := gRPCServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
					b.Error(err)
				}
			}()
			defer gRPCServer.Stop()

			// Initialize gRPC Client
			conn, err := grpc.NewClient(listener.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(math.MaxInt32)),
			)
			require.NoError(b, err)
			defer conn.Close()

			gRPCClient := storepb.NewStoreClient(conn)
			for _, batchSize := range batchSizes {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						benchmarkBatching(b, gRPCClient, userID, batchSize, series)
						require.NoError(b, err)
					}
				})
			}
		})
	}
}

func benchmarkBatching(b *testing.B, client storepb.StoreClient, userID string, batchSize int, expectedSeries int) {
	// Inject Tenant ID into context
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(cortex_tsdb.TenantIDExternalLabel, userID))

	req := &storepb.SeriesRequest{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: ".*"},
		},
		ResponseBatchSize: int64(batchSize), // This triggers batching in Thanos
	}

	stream, err := client.Series(ctx, req)
	require.NoError(b, err)

	got := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(b, err)

		if series := resp.GetSeries(); series != nil {
			got++
		} else if batch := resp.GetBatch(); batch != nil {
			got += len(batch.Series)
		}
	}

	if got != expectedSeries {
		b.Fatalf("expected %d series, got %d", expectedSeries, got)
	}
}

func generateBenchmarkBlock(b *testing.B, storageDir, userID string, numSeries, numSamples int) {
	userDir := filepath.Join(storageDir, userID)
	if err := os.MkdirAll(userDir, os.ModePerm); err != nil {
		b.Fatal(err)
	}

	tmpDir := b.TempDir()
	db, err := tsdb.Open(tmpDir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(b, err)
	defer db.Close()

	app := db.Appender(context.Background())

	for i := range numSeries {
		lbls := labels.FromStrings(
			labels.MetricName, "test_metric",
			"idx", fmt.Sprintf("%d", i),
			"job", "test_job",
			"instance", "localhost:9090",
		)

		for t := range numSamples {
			// Time in milliseconds, step 15s
			ts := int64(t * 15000)
			_, err := app.Append(0, lbls, ts, float64(i))
			require.NoError(b, err)
		}
	}

	require.NoError(b, app.Commit())
	require.NoError(b, db.Snapshot(userDir, true))
}
