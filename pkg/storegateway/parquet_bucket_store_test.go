package storegateway

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
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
