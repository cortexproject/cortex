package compactor

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestTimeShardingCompactor_Plan(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)

	tests := map[string]struct {
		ranges      []int64
		blocks      []*metadata.Meta
		expectedOut []ulid.ULID
		expectedErr error
	}{
		"the dir contains no blocks": {
			ranges:      []int64{20, 40, 60},
			blocks:      []*metadata.Meta{},
			expectedOut: nil,
		},
		"the dir contains some blocks, whose ordering by block ID doesn't match with the ordering by MinTime": {
			ranges: []int64{20, 40, 60},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20, Version: metadata.MetaVersion1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10, Version: metadata.MetaVersion1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 7, MaxTime: 15, Version: metadata.MetaVersion1}},
			},
			expectedOut: []ulid.ULID{block2, block3, block1},
		},
		"the dir contains a block larger then the largest range": {
			ranges: []int64{20, 40, 60},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20, Version: metadata.MetaVersion1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 80, Version: metadata.MetaVersion1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 7, MaxTime: 15, Version: metadata.MetaVersion1}},
			},
			expectedErr: fmt.Errorf("block %s with time range 0:80 is outside the largest expected range 0:60", block2.String()),
		},
		"the dir contains a block smaller then the largest range but misaligned": {
			ranges: []int64{20, 40, 60},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20, Version: metadata.MetaVersion1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 30, MaxTime: 40, Version: metadata.MetaVersion1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 50, MaxTime: 70, Version: metadata.MetaVersion1}},
			},
			expectedErr: fmt.Errorf("block %s with time range 50:70 is outside the largest expected range 0:60", block3.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create a temporary dir.
			tempDir, err := ioutil.TempDir(os.TempDir(), "blocks")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir) //nolint:errcheck

			// Create a meta file for each block.
			for _, meta := range testData.blocks {
				blockDir := filepath.Join(tempDir, meta.ULID.String())

				require.NoError(t, os.MkdirAll(blockDir, os.ModePerm))
				require.NoError(t, metadata.Write(nil, blockDir, meta))
			}

			c := NewTimeShardingCompactor(&tsdbCompactorMock{}, testData.ranges)
			actual, err := c.Plan(tempDir)
			require.Equal(t, testData.expectedErr, err)

			// Build the list of expected paths.
			var expected []string
			for _, id := range testData.expectedOut {
				expected = append(expected, filepath.Join(tempDir, id.String()))
			}

			assert.Equal(t, expected, actual)
		})
	}
}
