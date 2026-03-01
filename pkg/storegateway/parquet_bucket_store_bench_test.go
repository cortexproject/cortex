package storegateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcMetadata "google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/parquetconverter"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func BenchmarkParquetBucketStore_SeriesBatch(b *testing.B) {
	seriesNum := []int{100, 1000, 10000, 100000}
	samplePerSeries := 100
	batchSizes := []int{1, 10, 100, 1000, 10000}

	for _, series := range seriesNum {
		b.Run(fmt.Sprintf("series_%d", series), func(b *testing.B) {
			ctx := context.Background()
			tmpDir := b.TempDir()
			storageDir := filepath.Join(tmpDir, "storage")
			dataDir := filepath.Join(tmpDir, "data")
			userID := "user-1"

			// Initialize the BucketStore
			storageCfg := cortex_tsdb.BlocksStorageConfig{
				UsersScanner: users.UsersScannerConfig{
					Strategy:       users.UserScanStrategyList,
					UpdateInterval: time.Second,
				},
				Bucket: bucket.Config{
					Backend: "filesystem",
					Filesystem: filesystem.Config{
						Directory: storageDir,
					},
				},
				BucketStore: cortex_tsdb.BucketStoreConfig{
					SyncDir:                filepath.Join(tmpDir, "sync"),
					BucketStoreType:        "parquet",
					BlockDiscoveryStrategy: string(cortex_tsdb.RecursiveDiscovery),
				},
			}
			bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "test", log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(b, err)

			blockID := prepareParquetBlock(b, ctx, storageCfg, bucketClient, dataDir, userID, series, samplePerSeries)

			reg := prometheus.NewPedanticRegistry()
			stores, err := NewBucketStores(storageCfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bucketClient), defaultLimitsOverrides(nil), mockLoggingLevel(), log.NewNopLogger(), reg)
			require.NoError(b, err)

			// Start gRPC Server
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(b, err)

			gRPCServer := grpc.NewServer(
				grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
			)
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
					grpc.MaxCallRecvMsgSize(math.MaxInt32),
				),
			)
			require.NoError(b, err)
			defer conn.Close()

			gRPCClient := storepb.NewStoreClient(conn)
			for _, batchSize := range batchSizes {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						benchmarkBatchingForParquetBucketStore(b, gRPCClient, userID, batchSize, series, blockID)
					}
				})
			}
		})
	}
}

func prepareParquetBlock(b *testing.B, ctx context.Context, storageCfg cortex_tsdb.BlocksStorageConfig, bkt objstore.InstrumentedBucket, dataDir, userID string, numSeries, numSamples int) string {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()

	// Generate TSDB block
	generateBenchmarkBlock(b, dataDir, userID, numSeries, numSamples)

	userBucket := bucket.NewUserBucketClient("user-1", bkt, nil)
	srcBlockDir := filepath.Join(dataDir, userID)

	// Find blockID
	dirs, err := os.ReadDir(srcBlockDir)
	require.NoError(b, err)
	var blockID string
	for _, d := range dirs {
		if d.IsDir() {
			blockID = d.Name()
			break
		}
	}
	require.NotEmpty(b, blockID)

	blockPath := filepath.Join(srcBlockDir, blockID)
	metaFilePath := filepath.Join(blockPath, "meta.json")
	metaBytes, err := os.ReadFile(metaFilePath)
	require.NoError(b, err)

	var meta metadata.Meta
	err = json.Unmarshal(metaBytes, &meta)
	require.NoError(b, err)

	if meta.Thanos.Labels == nil {
		meta.Thanos.Labels = make(map[string]string)
	}
	meta.Thanos.Labels["replica"] = "0" // append dummy label to success block.Upload

	// Write thanos label appended meta.json
	newMetaBytes, err := json.Marshal(meta)
	require.NoError(b, err)
	err = os.WriteFile(metaFilePath, newMetaBytes, 0666)
	require.NoError(b, err)

	// Upload generated block to Storage
	err = block.Upload(ctx, logger, userBucket, blockPath, metadata.NoneFunc)
	require.NoError(b, err)

	convCfg := parquetconverter.Config{}
	flagext.DefaultValues(&convCfg)
	convCfg.ConversionInterval = time.Second // to convert quickly
	convCfg.DataDir = filepath.Join(dataDir, "converter-data")

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	b.Cleanup(func() { assert.NoError(b, closer.Close()) })

	convCfg.Ring.InstanceID = "parquet-converter-1"
	convCfg.Ring.InstanceAddr = "1.2.3.4"
	convCfg.Ring.KVStore.Mock = ringStore

	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true
	overrides := validation.NewOverrides(*limits, nil)

	// Create parquet converter
	converter, err := parquetconverter.NewConverter(convCfg, storageCfg, []int64{1, 2 * 3600 * 1000}, logger, reg, overrides)
	require.NoError(b, err)

	err = services.StartAndAwaitRunning(context.Background(), converter)
	require.NoError(b, err)
	defer services.StopAndAwaitTerminated(ctx, converter) // nolint:errcheck

	// check parquet converter mark file exist
	markerFile := filepath.Join(blockID, "parquet-converter-mark.json")
	require.Eventually(b, func() bool {
		exists, err := userBucket.Exists(ctx, markerFile)
		return err == nil && exists
	}, 10*time.Second, 100*time.Millisecond, "failed to wait for parquet conversion (marker file not found)")

	// check chunk parquet file exist
	existsChunks, err := userBucket.Exists(ctx, filepath.Join(blockID, "0.chunks.parquet"))
	require.NoError(b, err)
	require.True(b, existsChunks, "chunks.parquet file should exist")

	// check labels parquet file exist
	existsLabels, err := userBucket.Exists(ctx, filepath.Join(blockID, "0.labels.parquet"))
	require.NoError(b, err)
	require.True(b, existsLabels, "labels.parquet file should exist")

	return blockID
}

func benchmarkBatchingForParquetBucketStore(b *testing.B, client storepb.StoreClient, userID string, batchSize int, expectedSeries int, blockID string) {
	ctx := grpcMetadata.NewOutgoingContext(context.Background(), grpcMetadata.Pairs(cortex_tsdb.TenantIDExternalLabel, userID))
	ctx, err := user.InjectIntoGRPCRequest(user.InjectOrgID(ctx, userID))
	require.NoError(b, err)

	hintMatchers := []storepb.LabelMatcher{
		{
			Type:  storepb.LabelMatcher_RE,
			Name:  block.BlockIDLabel,
			Value: blockID,
		},
	}

	dataMatchers := []storepb.LabelMatcher{
		{
			Type:  storepb.LabelMatcher_RE,
			Name:  "__name__",
			Value: ".+",
		},
	}

	hints := &hintspb.SeriesRequestHints{
		BlockMatchers: hintMatchers,
	}
	hintsAny, err := types.MarshalAny(hints)
	require.NoError(b, err)

	req := &storepb.SeriesRequest{
		MinTime:           0,
		MaxTime:           math.MaxInt64,
		Matchers:          dataMatchers,
		ResponseBatchSize: int64(batchSize),
		Hints:             hintsAny,
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
