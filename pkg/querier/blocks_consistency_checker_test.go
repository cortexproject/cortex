package querier

import (
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/hintspb"

	"github.com/cortexproject/cortex/pkg/util/timeutil"
)

func TestBlocksConsistencyChecker_Check(t *testing.T) {
	now := time.Now()
	uploadGracePeriod := 10 * time.Minute
	deletionGracePeriod := 5 * time.Minute

	block1 := ulid.MustNew(uint64(timeutil.TimeToMillis(now.Add(-uploadGracePeriod*2))), nil)
	block2 := ulid.MustNew(uint64(timeutil.TimeToMillis(now.Add(-uploadGracePeriod*3))), nil)
	block3 := ulid.MustNew(uint64(timeutil.TimeToMillis(now.Add(-uploadGracePeriod*4))), nil)
	blockRecentlyUploaded := ulid.MustNew(uint64(timeutil.TimeToMillis(now)), nil)

	tests := map[string]struct {
		expectedBlocks     []ulid.ULID
		knownDeletionMarks map[ulid.ULID]*metadata.DeletionMark
		queriedBlocks      map[string][]hintspb.Block
		expectedErr        error
	}{
		"no expected blocks": {
			expectedBlocks:     []ulid.ULID{},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{},
			queriedBlocks:      map[string][]hintspb.Block{},
		},
		"all expected blocks have been queried from a single store-gateway": {
			expectedBlocks:     []ulid.ULID{block1, block2},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}, {Id: block2.String()}},
			},
		},
		"all expected blocks have been queried from multiple store-gateway": {
			expectedBlocks:     []ulid.ULID{block1, block2},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}},
				"2.2.2.2": {{Id: block2.String()}},
			},
		},
		"store-gateway has queried more blocks than expected": {
			expectedBlocks:     []ulid.ULID{block1, block2},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}},
				"2.2.2.2": {{Id: block2.String()}, {Id: block3.String()}},
			},
		},
		"store-gateway has queried less blocks than expected": {
			expectedBlocks:     []ulid.ULID{block1, block2, block3},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}},
				"2.2.2.2": {{Id: block3.String()}},
			},
			expectedErr: fmt.Errorf("consistency check failed because of non-queried blocks: %s", block2.String()),
		},
		"store-gateway has queried less blocks than expected, but the missing block has been recently uploaded": {
			expectedBlocks:     []ulid.ULID{block1, block2, blockRecentlyUploaded},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}},
				"2.2.2.2": {{Id: block2.String()}},
			},
		},
		"store-gateway has queried less blocks than expected and the missing block has been recently marked for deletion": {
			expectedBlocks: []ulid.ULID{block1, block2, block3},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{
				block3: {DeletionTime: now.Add(-deletionGracePeriod / 2).Unix()},
			},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}},
				"2.2.2.2": {{Id: block2.String()}},
			},
			expectedErr: fmt.Errorf("consistency check failed because of non-queried blocks: %s", block3.String()),
		},
		"store-gateway has queried less blocks than expected and the missing block has been marked for deletion long time ago": {
			expectedBlocks: []ulid.ULID{block1, block2, block3},
			knownDeletionMarks: map[ulid.ULID]*metadata.DeletionMark{
				block3: {DeletionTime: now.Add(-deletionGracePeriod * 2).Unix()},
			},
			queriedBlocks: map[string][]hintspb.Block{
				"1.1.1.1": {{Id: block1.String()}},
				"2.2.2.2": {{Id: block2.String()}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			c := NewBlocksConsistencyChecker(uploadGracePeriod, deletionGracePeriod, reg)

			err := c.Check(testData.expectedBlocks, testData.knownDeletionMarks, testData.queriedBlocks)
			assert.Equal(t, testData.expectedErr, err)
			assert.Equal(t, float64(1), testutil.ToFloat64(c.checksTotal))

			if testData.expectedErr != nil {
				assert.Equal(t, float64(1), testutil.ToFloat64(c.checksFailed))
			} else {
				assert.Equal(t, float64(0), testutil.ToFloat64(c.checksFailed))
			}
		})
	}
}
