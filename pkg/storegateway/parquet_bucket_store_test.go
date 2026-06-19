package storegateway

import (
	"bytes"
	"context"
	"io"
	"math"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcMetadata "google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/users"
)

type mockParquetFileView struct {
	parquet_storage.ParquetFileView
	file *parquet.File
}

func (m *mockParquetFileView) Schema() *parquet.Schema {
	return m.file.Schema()
}

type mockShard struct {
	parquet_storage.ParquetShard
	fileView parquet_storage.ParquetFileView
}

func (m *mockShard) LabelsFile() parquet_storage.ParquetFileView {
	return m.fileView
}

func createTestParquetBlock(t *testing.T, hasHash bool) *parquetBlock {
	t.Helper()

	var buf bytes.Buffer
	var err error

	if hasHash {
		// v2 and higher blocks
		type RowWithHash struct {
			SeriesHash string `parquet:"s_series_hash"`
			Label      string `parquet:"l_job"`
		}

		w := parquet.NewGenericWriter[RowWithHash](&buf)
		_, err = w.Write([]RowWithHash{{SeriesHash: "hash1", Label: "node-1"}})
		require.NoError(t, err)
		require.NoError(t, w.Close())
	} else {
		// v1 block
		type RowWithoutHash struct {
			Label string `parquet:"l_job"`
		}

		w := parquet.NewGenericWriter[RowWithoutHash](&buf)
		_, err = w.Write([]RowWithoutHash{{Label: "node-1"}})
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}

	readBuf := bytes.NewReader(buf.Bytes())
	f, err := parquet.OpenFile(readBuf, readBuf.Size())
	require.NoError(t, err)

	return &parquetBlock{
		shard: &mockShard{
			fileView: &mockParquetFileView{file: f},
		},
	}
}

func Test_AllParquetBlocksHaveHashColumn(t *testing.T) {
	tests := []struct {
		description string
		setup       func() []*parquetBlock
		expected    bool
	}{
		{
			description: "returns true when all blocks have hash column",
			setup: func() []*parquetBlock {
				return []*parquetBlock{
					createTestParquetBlock(t, true),
					createTestParquetBlock(t, true),
					createTestParquetBlock(t, true),
				}
			},
			expected: true,
		},
		{
			description: "returns false when mixed block versions exist",
			setup: func() []*parquetBlock {
				return []*parquetBlock{
					createTestParquetBlock(t, true),
					createTestParquetBlock(t, false),
					createTestParquetBlock(t, true),
				}
			},
			expected: false,
		},
		{
			description: "returns false when no blocks have hash column",
			setup: func() []*parquetBlock {
				return []*parquetBlock{
					createTestParquetBlock(t, false),
					createTestParquetBlock(t, false),
				}
			},
			expected: false,
		},
		{
			description: "returns true for empty block list",
			setup: func() []*parquetBlock {
				return []*parquetBlock{}
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			blocks := tc.setup()
			actual := allParquetBlocksHaveHashColumn(blocks)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestParquetBucketStore_buildSelectHints(t *testing.T) {
	const (
		minT = 1000
		maxT = 2000
	)

	tests := []struct {
		description          string
		honorProjectionHints bool
		queryHints           *storepb.QueryHints
		shards               []*parquetBlock
		expectedHints        *prom_storage.SelectHints
	}{
		{
			description:          "honorProjectionHints=false, should ignore query hints",
			honorProjectionHints: false,
			queryHints: &storepb.QueryHints{
				ProjectionInclude: true,
				ProjectionLabels:  []string{"job"},
			},
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: false,
				ProjectionLabels:  nil,
			},
		},
		{
			description:          "honorProjectionHints=true, V2 blocks, should enable projection and append hash column",
			honorProjectionHints: true,
			queryHints: &storepb.QueryHints{
				ProjectionInclude: true,
				ProjectionLabels:  []string{"job"},
			},
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
				createTestParquetBlock(t, true),
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: true,
				ProjectionLabels:  []string{"job", schema.SeriesHashColumn},
			},
		},
		{
			description:          "honorProjectionHints=true, V2 blocks, should not duplicate hash column if already present",
			honorProjectionHints: true,
			queryHints: &storepb.QueryHints{
				ProjectionInclude: true,
				ProjectionLabels:  []string{"job", schema.SeriesHashColumn},
			},
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: true,
				ProjectionLabels:  []string{"job", schema.SeriesHashColumn},
			},
		},
		{
			description:          "honorProjectionHints=true, Mixed V1/V2 blocks, should reset projection",
			honorProjectionHints: true,
			queryHints: &storepb.QueryHints{
				ProjectionInclude: true,
				ProjectionLabels:  []string{"job"},
			},
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
				createTestParquetBlock(t, false), // v1
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: false,
				ProjectionLabels:  nil,
			},
		},
		{
			description:          "honorProjectionHints=true, exclude projection (ProjectionInclude=false), should reset to full scan",
			honorProjectionHints: true,
			queryHints: &storepb.QueryHints{
				ProjectionInclude: false,
				ProjectionLabels:  []string{"job"},
			},
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: false,
				ProjectionLabels:  nil, // Non-include projections fall back to a full scan
			},
		},
		{
			description:          "honorProjectionHints=true, nil query hints, should return default hints",
			honorProjectionHints: true,
			queryHints:           nil,
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: false,
				ProjectionLabels:  nil,
			},
		},
		{
			description:          "honorProjectionHints=true, Empty projection labels, should add hash column only",
			honorProjectionHints: true,
			queryHints: &storepb.QueryHints{
				ProjectionInclude: true,
				ProjectionLabels:  []string{},
			},
			shards: []*parquetBlock{
				createTestParquetBlock(t, true),
			},
			expectedHints: &prom_storage.SelectHints{
				Start:             minT,
				End:               maxT,
				ProjectionInclude: true,
				ProjectionLabels:  []string{schema.SeriesHashColumn},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			store := &parquetBucketStore{
				honorProjectionHints: tc.honorProjectionHints,
			}
			shards := tc.shards
			hints := store.buildSelectHints(tc.queryHints, shards, minT, maxT)
			require.Equal(t, tc.expectedHints, hints)
		})
	}
}

func TestParquetBucketStore_Series_ProjectionHints(t *testing.T) {
	const (
		userID     = "user-1"
		numSeries  = 100
		numSamples = 10
	)

	ctx := context.Background()
	tmpDir := t.TempDir()
	storageDir := filepath.Join(tmpDir, "storage")
	dataDir := filepath.Join(tmpDir, "data")

	storageCfg := cortex_tsdb.BlocksStorageConfig{
		UsersScanner: users.UsersScannerConfig{
			Strategy:       users.UserScanStrategyList,
			UpdateInterval: time.Second,
		},
		Bucket: bucket.Config{
			Backend:    "filesystem",
			Filesystem: filesystem.Config{Directory: storageDir},
		},
		BucketStore: cortex_tsdb.BucketStoreConfig{
			SyncDir:                filepath.Join(tmpDir, "sync"),
			BucketStoreType:        "parquet",
			BlockDiscoveryStrategy: string(cortex_tsdb.RecursiveDiscovery),
			// Must be > 0, otherwise the per-query errgroup limit becomes 0 and
			// errGroup.Go blocks forever, hanging the Series handler.
			ParquetQueryConcurrency: 4,
		},
	}

	bucketClient, err := bucket.NewClient(ctx, storageCfg.Bucket, nil, "test", log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Generated series carry these labels: __name__=test_metric, idx=<i>, job=test_job, instance=localhost:9090.
	blockID := prepareParquetBlock(t, ctx, storageCfg, bucketClient, dataDir, userID, numSeries, numSamples)

	startGRPCServer := func(honorProjectionHints bool) (storepb.StoreClient, func()) {
		cfg := storageCfg
		cfg.BucketStore.HonorProjectionHints = honorProjectionHints

		reg := prometheus.NewPedanticRegistry()
		stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bucketClient), defaultLimitsOverrides(nil), mockLoggingLevel(), log.NewNopLogger(), reg)
		require.NoError(t, err)

		listener, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)

		gRPCServer := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
		storepb.RegisterStoreServer(gRPCServer, stores)
		go func() {
			if err := gRPCServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
				t.Error(err)
			}
		}()

		conn, err := grpc.NewClient(listener.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		)
		require.NoError(t, err)

		return storepb.NewStoreClient(conn), func() {
			_ = conn.Close()
			gRPCServer.Stop()
		}
	}

	// querySeriesLabels issues a Series request and returns the union of all label names seen across
	// the returned series.
	querySeriesLabels := func(t *testing.T, client storepb.StoreClient, projectionLabels []string) map[string]struct{} {
		reqCtx := grpcMetadata.NewOutgoingContext(context.Background(), grpcMetadata.Pairs(cortex_tsdb.TenantIDExternalLabel, userID))
		reqCtx, err := user.InjectIntoGRPCRequest(user.InjectOrgID(reqCtx, userID))
		require.NoError(t, err)

		seriesHints := &hintspb.SeriesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: block.BlockIDLabel, Value: blockID}},
		}
		hintsAny, err := types.MarshalAny(seriesHints)
		require.NoError(t, err)

		req := &storepb.SeriesRequest{
			MinTime:           0,
			MaxTime:           math.MaxInt64,
			Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}},
			ResponseBatchSize: 1000,
			Hints:             hintsAny,
		}
		if len(projectionLabels) > 0 {
			req.QueryHints = &storepb.QueryHints{ProjectionInclude: true, ProjectionLabels: projectionLabels}
		}

		stream, err := client.Series(reqCtx, req)
		require.NoError(t, err)

		seen := map[string]struct{}{}
		seriesCount := 0
		collect := func(zlabels []labelpb.ZLabel) {
			seriesCount++
			for _, l := range zlabels {
				seen[l.Name] = struct{}{}
			}
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			if s := resp.GetSeries(); s != nil {
				collect(s.Labels)
			} else if b := resp.GetBatch(); b != nil {
				for _, s := range b.Series {
					collect(s.Labels)
				}
			}
		}
		require.Equal(t, numSeries, seriesCount, "all series should be returned regardless of projection")
		return seen
	}

	client, stop := startGRPCServer(true)
	defer stop()

	testCases := []struct {
		name             string
		projectionLabels []string
		wantPresent      []string
		wantAbsent       []string
	}{
		{
			name:        "without projection hints returns the full label set",
			wantPresent: []string{"__name__", "job", "instance", "idx"},
		},
		{
			name:             "with projection hints only materializes requested labels",
			projectionLabels: []string{"job"},
			wantPresent:      []string{"job"},
			wantAbsent:       []string{"__name__", "instance", "idx"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			labels := querySeriesLabels(t, client, tc.projectionLabels)
			for _, l := range tc.wantPresent {
				require.Contains(t, labels, l, "expected label %q to be materialized", l)
			}
			for _, l := range tc.wantAbsent {
				require.NotContains(t, labels, l, "expected label %q to be projected away", l)
			}
		})
	}
}
