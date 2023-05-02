package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestShuffleShardingGrouper_Groups(t *testing.T) {
	block1hto2hExt1Ulid := ulid.MustNew(1, nil)
	block3hto4hExt1Ulid := ulid.MustNew(2, nil)
	block0hto1hExt1Ulid := ulid.MustNew(3, nil)
	block2hto3hExt1Ulid := ulid.MustNew(4, nil)
	block1hto2hExt2Ulid := ulid.MustNew(5, nil)
	block0hto1hExt2Ulid := ulid.MustNew(6, nil)
	block0to1hExt3Ulid := ulid.MustNew(7, nil)
	block4hto6hExt2Ulid := ulid.MustNew(8, nil)
	block6hto8hExt2Ulid := ulid.MustNew(9, nil)
	block1hto2hExt1UlidCopy := ulid.MustNew(10, nil)
	block0hto20hExt1Ulid := ulid.MustNew(11, nil)
	block21hto40hExt1Ulid := ulid.MustNew(12, nil)
	block21hto40hExt1UlidCopy := ulid.MustNew(13, nil)
	block0hto45mExt1Ulid := ulid.MustNew(14, nil)
	block0hto1h30mExt1Ulid := ulid.MustNew(15, nil)
	blocklast1hExt1Ulid := ulid.MustNew(16, nil)
	blocklast1hExt1UlidCopy := ulid.MustNew(17, nil)

	blocks :=
		map[ulid.ULID]*metadata.Meta{
			block1hto2hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block1hto2hExt1Ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block1hto2hExt1UlidCopy: {
				BlockMeta: tsdb.BlockMeta{ULID: block1hto2hExt1UlidCopy, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block3hto4hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block3hto4hExt1Ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 4 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block0hto1hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0hto1hExt1Ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block2hto3hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block2hto3hExt1Ulid, MinTime: 2 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block1hto2hExt2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block1hto2hExt2Ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block0hto1hExt2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0hto1hExt2Ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block0to1hExt3Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0to1hExt3Ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "3"}},
			},
			block4hto6hExt2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block4hto6hExt2Ulid, MinTime: 4 * time.Hour.Milliseconds(), MaxTime: 6 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block6hto8hExt2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block6hto8hExt2Ulid, MinTime: 6 * time.Hour.Milliseconds(), MaxTime: 8 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block0hto20hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0hto20hExt1Ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 20 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block21hto40hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block21hto40hExt1Ulid, MinTime: 21 * time.Hour.Milliseconds(), MaxTime: 40 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block21hto40hExt1UlidCopy: {
				BlockMeta: tsdb.BlockMeta{ULID: block21hto40hExt1UlidCopy, MinTime: 21 * time.Hour.Milliseconds(), MaxTime: 40 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block0hto45mExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0hto45mExt1Ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 45 * time.Minute.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block0hto1h30mExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0hto1h30mExt1Ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1*time.Hour.Milliseconds() + 30*time.Minute.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			blocklast1hExt1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: blocklast1hExt1Ulid, MinTime: int64(ulid.Now()) - 1*time.Hour.Milliseconds(), MaxTime: int64(ulid.Now())},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			blocklast1hExt1UlidCopy: {
				BlockMeta: tsdb.BlockMeta{ULID: blocklast1hExt1UlidCopy, MinTime: int64(ulid.Now()) - 1*time.Hour.Milliseconds(), MaxTime: int64(ulid.Now())},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
		}

	testCompactorID := "test-compactor"
	otherCompactorID := "other-compactor"

	tests := map[string]struct {
		concurrency   int
		ranges        []time.Duration
		blocks        map[ulid.ULID]*metadata.Meta
		visitedBlocks []struct {
			id          ulid.ULID
			compactorID string
			isExpired   bool
		}
		expected        [][]ulid.ULID
		metrics         string
		noCompactBlocks map[ulid.ULID]*metadata.NoCompactMark
	}{
		"test basic grouping": {
			concurrency: 3,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt2Ulid, block0hto1hExt2Ulid},
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid},
				{block3hto4hExt1Ulid, block2hto3hExt1Ulid},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 3
`,
		},
		"test no compaction": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid], block0to1hExt3Ulid: blocks[block0to1hExt3Ulid]},
			expected:    [][]ulid.ULID{},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 0
`,
		},
		"test smallest range first": {
			concurrency: 3,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block4hto6hExt2Ulid: blocks[block4hto6hExt2Ulid], block6hto8hExt2Ulid: blocks[block6hto8hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid},
				{block3hto4hExt1Ulid, block2hto3hExt1Ulid},
				{block4hto6hExt2Ulid, block6hto8hExt2Ulid},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 3
`,
		},
		"test oldest min time first": {
			concurrency: 2,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt1UlidCopy: blocks[block1hto2hExt1UlidCopy]},
			expected: [][]ulid.ULID{
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid, block1hto2hExt1UlidCopy},
				{block3hto4hExt1Ulid, block2hto3hExt1Ulid},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 2
`,
		},
		"test overlapping blocks": {
			concurrency: 1,
			ranges:      []time.Duration{20 * time.Hour, 40 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block0hto20hExt1Ulid: blocks[block0hto20hExt1Ulid], block21hto40hExt1Ulid: blocks[block21hto40hExt1Ulid], block21hto40hExt1UlidCopy: blocks[block21hto40hExt1UlidCopy]},
			expected: [][]ulid.ULID{
				{block21hto40hExt1Ulid, block21hto40hExt1UlidCopy},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 1
`,
		},
		"test imperfect maxTime blocks": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block0hto1h30mExt1Ulid: blocks[block0hto1h30mExt1Ulid], block0hto45mExt1Ulid: blocks[block0hto45mExt1Ulid]},
			expected: [][]ulid.ULID{
				{block0hto45mExt1Ulid, block0hto1h30mExt1Ulid},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 1
`,
		},
		"test prematurely created blocks": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{blocklast1hExt1UlidCopy: blocks[blocklast1hExt1UlidCopy], blocklast1hExt1Ulid: blocks[blocklast1hExt1Ulid]},
			expected:    [][]ulid.ULID{},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 0
`,
		},
		"test group with all blocks visited": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid},
			},
			visitedBlocks: []struct {
				id          ulid.ULID
				compactorID string
				isExpired   bool
			}{
				{id: block1hto2hExt2Ulid, compactorID: otherCompactorID, isExpired: false},
				{id: block0hto1hExt2Ulid, compactorID: otherCompactorID, isExpired: false},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 1
`,
		},
		"test group with one block visited": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid},
			},
			visitedBlocks: []struct {
				id          ulid.ULID
				compactorID string
				isExpired   bool
			}{
				{id: block1hto2hExt2Ulid, compactorID: otherCompactorID, isExpired: false},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 1
`,
		},
		"test group block visit marker file expired": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt2Ulid, block0hto1hExt2Ulid},
			},
			visitedBlocks: []struct {
				id          ulid.ULID
				compactorID string
				isExpired   bool
			}{
				{id: block1hto2hExt2Ulid, compactorID: otherCompactorID, isExpired: true},
				{id: block0hto1hExt2Ulid, compactorID: otherCompactorID, isExpired: true},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 1
`,
		},
		"test group with one block visited by current compactor": {
			concurrency: 1,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt2Ulid, block0hto1hExt2Ulid},
			},
			visitedBlocks: []struct {
				id          ulid.ULID
				compactorID string
				isExpired   bool
			}{
				{id: block1hto2hExt2Ulid, compactorID: testCompactorID, isExpired: false},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 1
`,
		},
		"test basic grouping with concurrency 2": {
			concurrency: 2,
			ranges:      []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block3hto4hExt1Ulid: blocks[block3hto4hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt2Ulid, block0hto1hExt2Ulid},
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 2
`,
		},
		"test should skip block with no compact marker": {
			concurrency: 2,
			ranges:      []time.Duration{4 * time.Hour},
			blocks:      map[ulid.ULID]*metadata.Meta{block1hto2hExt1Ulid: blocks[block1hto2hExt1Ulid], block0hto1hExt1Ulid: blocks[block0hto1hExt1Ulid], block1hto2hExt2Ulid: blocks[block1hto2hExt2Ulid], block0hto1hExt2Ulid: blocks[block0hto1hExt2Ulid], block2hto3hExt1Ulid: blocks[block2hto3hExt1Ulid]},
			expected: [][]ulid.ULID{
				{block1hto2hExt2Ulid, block0hto1hExt2Ulid},
				{block1hto2hExt1Ulid, block0hto1hExt1Ulid},
			},
			metrics: `# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted.
        	          # TYPE cortex_compactor_remaining_planned_compactions gauge
        	          cortex_compactor_remaining_planned_compactions 2
`,
			noCompactBlocks: map[ulid.ULID]*metadata.NoCompactMark{block2hto3hExt1Ulid: {}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			compactorCfg := &Config{
				BlockRanges: testData.ranges,
			}

			limits := &validation.Limits{}
			overrides, err := validation.NewOverrides(*limits, nil)
			require.NoError(t, err)

			// Setup mocking of the ring so that the grouper will own all the shards
			rs := ring.ReplicationSet{
				Instances: []ring.InstanceDesc{
					{Addr: "test-addr"},
				},
			}
			subring := &RingMock{}
			subring.On("GetAllHealthy", mock.Anything).Return(rs, nil)
			subring.On("Get", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(rs, nil)

			ring := &RingMock{}
			ring.On("ShuffleShard", mock.Anything, mock.Anything).Return(subring, nil)

			registerer := prometheus.NewPedanticRegistry()
			remainingPlannedCompactions := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
				Name: "cortex_compactor_remaining_planned_compactions",
				Help: "Total number of plans that remain to be compacted.",
			})
			blockVisitMarkerReadFailed := prometheus.NewCounter(prometheus.CounterOpts{})
			blockVisitMarkerWriteFailed := prometheus.NewCounter(prometheus.CounterOpts{})
			partitionedGroupInfoReadFailed := prometheus.NewCounter(prometheus.CounterOpts{})
			partitionedGroupInfoWriteFailed := prometheus.NewCounter(prometheus.CounterOpts{})

			bkt := &bucket.ClientMock{}
			blockVisitMarkerTimeout := 5 * time.Minute
			for _, visitedBlock := range testData.visitedBlocks {
				visitMarkerFile := getBlockVisitMarkerFile(visitedBlock.id.String(), 0)
				expireTime := time.Now()
				if visitedBlock.isExpired {
					expireTime = expireTime.Add(-1 * blockVisitMarkerTimeout)
				}
				blockVisitMarker := BlockVisitMarker{
					CompactorID: visitedBlock.compactorID,
					VisitTime:   expireTime.Unix(),
					Version:     VisitMarkerVersion1,
				}
				visitMarkerFileContent, _ := json.Marshal(blockVisitMarker)
				bkt.MockGet(visitMarkerFile, string(visitMarkerFileContent), nil)
			}
			bkt.MockUpload(mock.Anything, nil)
			bkt.MockGet(mock.Anything, "", nil)

			noCompactFilter := func() map[ulid.ULID]*metadata.NoCompactMark {
				return testData.noCompactBlocks
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := NewShuffleShardingGrouper(
				ctx,
				nil,
				objstore.WithNoopInstr(bkt),
				false, // Do not accept malformed indexes
				true,  // Enable vertical compaction
				registerer,
				nil,
				nil,
				nil,
				remainingPlannedCompactions,
				metadata.NoneFunc,
				*compactorCfg,
				ring,
				"test-addr",
				testCompactorID,
				overrides,
				"",
				10,
				3,
				testData.concurrency,
				blockVisitMarkerTimeout,
				blockVisitMarkerReadFailed,
				blockVisitMarkerWriteFailed,
				partitionedGroupInfoReadFailed,
				partitionedGroupInfoWriteFailed,
				noCompactFilter,
			)
			actual, err := g.Groups(testData.blocks)
			require.NoError(t, err)
			require.Len(t, actual, len(testData.expected))

			for idx, expectedIDs := range testData.expected {
				assert.Equal(t, expectedIDs, actual[idx].IDs())
			}

			err = testutil.GatherAndCompare(registerer, bytes.NewBufferString(testData.metrics), "cortex_compactor_remaining_planned_compactions")
			require.NoError(t, err)
		})
	}
}

func TestGroupBlocksByCompactableRanges(t *testing.T) {
	block1Ulid := ulid.MustNew(1, nil)
	block2Ulid := ulid.MustNew(2, nil)
	block3Ulid := ulid.MustNew(3, nil)
	block4Ulid := ulid.MustNew(4, nil)
	block5Ulid := ulid.MustNew(5, nil)
	block6Ulid := ulid.MustNew(6, nil)
	block7Ulid := ulid.MustNew(7, nil)
	block8Ulid := ulid.MustNew(8, nil)
	block9Ulid := ulid.MustNew(9, nil)

	defaultPartitionInfo := &metadata.PartitionInfo{
		PartitionedGroupID: 0,
		PartitionCount:     1,
		PartitionID:        0,
	}

	partition3ID0 := &metadata.PartitionInfo{
		PartitionedGroupID: uint32(12345),
		PartitionCount:     3,
		PartitionID:        0,
	}

	partition3ID1 := &metadata.PartitionInfo{
		PartitionedGroupID: uint32(12345),
		PartitionCount:     3,
		PartitionID:        1,
	}

	partition3ID2 := &metadata.PartitionInfo{
		PartitionedGroupID: uint32(12345),
		PartitionCount:     3,
		PartitionID:        2,
	}

	partition2ID0 := &metadata.PartitionInfo{
		PartitionedGroupID: uint32(54321),
		PartitionCount:     2,
		PartitionID:        0,
	}

	partition2ID1 := &metadata.PartitionInfo{
		PartitionedGroupID: uint32(54321),
		PartitionCount:     2,
		PartitionID:        1,
	}

	tests := map[string]struct {
		ranges   []int64
		blocks   []*metadata.Meta
		expected []blocksGroup
	}{
		"no input blocks": {
			ranges:   []int64{20},
			blocks:   nil,
			expected: nil,
		},
		"only 1 block in input": {
			ranges: []int64{20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: nil,
		},
		"only 1 block for each range (single range)": {
			ranges: []int64{20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 40, MaxTime: 60}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: nil,
		},
		"only 1 block for each range (multiple ranges)": {
			ranges: []int64{10, 20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 40, MaxTime: 60}},
			},
			expected: nil,
		},
		"input blocks can be compacted on the 1st range only": {
			ranges: []int64{20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 25, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 40, MaxTime: 50}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6Ulid, MinTime: 50, MaxTime: 60}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block7Ulid, MinTime: 60, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 25, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 40, MaxTime: 50}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block6Ulid, MinTime: 50, MaxTime: 60}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"input blocks can be compacted on the 2nd range only": {
			ranges: []int64{10, 20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 40, MaxTime: 60}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 60, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6Ulid, MinTime: 70, MaxTime: 80}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
				{rangeStart: 60, rangeEnd: 80, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 60, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block6Ulid, MinTime: 70, MaxTime: 80}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"input blocks can be compacted on a mix of 1st and 2nd ranges, guaranteeing no overlaps and giving preference to smaller ranges": {
			ranges: []int64{10, 20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 7, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6Ulid, MinTime: 40, MaxTime: 60}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block7Ulid, MinTime: 60, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block8Ulid, MinTime: 70, MaxTime: 80}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block9Ulid, MinTime: 75, MaxTime: 80}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 10, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 7, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
				{rangeStart: 70, rangeEnd: 80, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block8Ulid, MinTime: 70, MaxTime: 80}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block9Ulid, MinTime: 75, MaxTime: 80}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"input blocks have already been compacted with the largest range": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 40, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: nil,
		},
		"input blocks match the largest range but can be compacted because overlapping": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 40, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 80, rangeEnd: 120, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should be NOT considered for 1st level compaction": {
			ranges: []int64{20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 10, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should BE considered for 2nd level compaction": {
			ranges: []int64{20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 10, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 20, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 10, MaxTime: 30}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 20, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"a block with time range larger then the largest compaction range should NOT be considered for compaction": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 40}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 30, MaxTime: 150}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}}, // This block is larger then the largest compaction range.
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 40, MaxTime: 70}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 80, rangeEnd: 120, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"a group with all blocks having same partitioned group id should be ignored": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID0}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID2}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 10, MaxTime: 20}},
			},
			expected: []blocksGroup{
				{rangeStart: 10, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 10, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 10, MaxTime: 20}},
				}},
			},
		},
		"a group with all blocks having partitioned group id is 0 should not be ignored": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 10, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
				{rangeStart: 10, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{PartitionInfo: defaultPartitionInfo}},
				}},
			},
		},
		"a group with blocks from two different partitioned groups": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID0}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID2}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 10, MaxTime: 20, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition2ID0}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 10, MaxTime: 20, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition2ID1}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID0}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID1}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3Ulid, MinTime: 0, MaxTime: 10, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition3ID2}},
					{BlockMeta: tsdb.BlockMeta{ULID: block4Ulid, MinTime: 10, MaxTime: 20, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition2ID0}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5Ulid, MinTime: 10, MaxTime: 20, Compaction: tsdb.BlockMetaCompaction{Level: 2}}, Thanos: metadata.Thanos{PartitionInfo: partition2ID1}},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, groupBlocksByCompactableRanges(testData.blocks, testData.ranges))
		})
	}
}

func TestGroupBlocksByRange(t *testing.T) {
	tests := map[string]struct {
		timeRange int64
		blocks    []*metadata.Meta
		expected  []blocksGroup
	}{
		"no input blocks": {
			timeRange: 20,
			blocks:    nil,
			expected:  nil,
		},
		"only 1 block in input": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
			},
		},
		"only 1 block per range": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				}},
			},
		},
		"multiple blocks per range": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
				}},
			},
		},
		"a block with time range larger then the range should be excluded": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}}, // This block is larger then the range.
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
		"blocks with different time ranges but all fitting within the input range": {
			timeRange: 40,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, groupBlocksByRange(testData.blocks, testData.timeRange))
		})
	}
}

func TestBlocksGroup_overlaps(t *testing.T) {
	tests := []struct {
		first    blocksGroup
		second   blocksGroup
		expected bool
	}{
		{
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 20, rangeEnd: 30},
			expected: false,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 19, rangeEnd: 30},
			expected: true,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 21},
			second:   blocksGroup{rangeStart: 20, rangeEnd: 30},
			expected: true,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 12, rangeEnd: 18},
			expected: true,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.first.overlaps(tc.second))
		assert.Equal(t, tc.expected, tc.second.overlaps(tc.first))
	}
}

func TestGroupPartitioning(t *testing.T) {
	t0block1Ulid := ulid.MustNew(1, nil)
	t0block2Ulid := ulid.MustNew(2, nil)
	t0block3Ulid := ulid.MustNew(3, nil)
	t1block1Ulid := ulid.MustNew(4, nil)
	t1block2Ulid := ulid.MustNew(5, nil)
	t2block1Ulid := ulid.MustNew(6, nil)
	t2block2Ulid := ulid.MustNew(7, nil)
	t2block3Ulid := ulid.MustNew(8, nil)
	t2block4Ulid := ulid.MustNew(9, nil)
	t3block1Ulid := ulid.MustNew(10, nil)
	t3block2Ulid := ulid.MustNew(11, nil)
	t3block3Ulid := ulid.MustNew(12, nil)
	t3block4Ulid := ulid.MustNew(13, nil)
	t3block5Ulid := ulid.MustNew(14, nil)
	t3block6Ulid := ulid.MustNew(15, nil)
	t3block7Ulid := ulid.MustNew(16, nil)
	t3block8Ulid := ulid.MustNew(17, nil)
	t4block1Ulid := ulid.MustNew(18, nil)
	t5block1Ulid := ulid.MustNew(19, nil)

	blocks :=
		map[ulid.ULID]*metadata.Meta{
			t0block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t0block1Ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t0block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t0block2Ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t0block3Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t0block3Ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t1block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t1block1Ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 5 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t1block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t1block2Ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 5 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block1Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block2Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block3Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block3Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block4Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block4Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block1Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block2Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block3Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block3Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block4Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block4Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block5Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block5Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block6Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block6Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block7Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block7Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block8Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block8Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t4block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t4block1Ulid, MinTime: 9 * time.Hour.Milliseconds(), MaxTime: 15 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t5block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t5block1Ulid, MinTime: 15 * time.Hour.Milliseconds(), MaxTime: 21 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
		}

	testCompactorID := "test-compactor"

	tests := map[string]struct {
		ranges      []time.Duration
		rangeStart  int64
		rangeEnd    int64
		indexSize   int64
		indexLimit  int64
		seriesCount int64
		seriesLimit int64
		blocks      map[*metadata.Meta]int
		expected    struct {
			partitionCount int
			partitions     map[int][]ulid.ULID
		}
	}{
		"test blocks generated by partition": {
			ranges:     []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart: 1 * time.Hour.Milliseconds(),
			rangeEnd:   9 * time.Hour.Milliseconds(),
			indexSize:  int64(14),
			indexLimit: int64(64),
			blocks: map[*metadata.Meta]int{
				blocks[t1block1Ulid]: 0, blocks[t1block2Ulid]: 1,
				blocks[t2block1Ulid]: 0, blocks[t2block2Ulid]: 1, blocks[t2block3Ulid]: 2, blocks[t2block4Ulid]: 3,
				blocks[t3block1Ulid]: 0, blocks[t3block2Ulid]: 1, blocks[t3block3Ulid]: 2, blocks[t3block4Ulid]: 3,
				blocks[t3block5Ulid]: 4, blocks[t3block6Ulid]: 5, blocks[t3block7Ulid]: 6, blocks[t3block8Ulid]: 7},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 4,
				partitions: map[int][]ulid.ULID{
					0: {t1block1Ulid, t2block1Ulid, t3block1Ulid, t3block5Ulid},
					1: {t1block2Ulid, t2block2Ulid, t3block2Ulid, t3block6Ulid},
					2: {t1block1Ulid, t2block3Ulid, t3block3Ulid, t3block7Ulid},
					3: {t1block2Ulid, t2block4Ulid, t3block4Ulid, t3block8Ulid},
				},
			},
		},
		"test all level 1 blocks": {
			ranges:     []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart: 1 * time.Hour.Milliseconds(),
			rangeEnd:   9 * time.Hour.Milliseconds(),
			indexSize:  int64(30),
			indexLimit: int64(64),
			blocks: map[*metadata.Meta]int{
				blocks[t0block1Ulid]: 0, blocks[t0block2Ulid]: 0, blocks[t0block3Ulid]: 0,
			},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 2,
				partitions: map[int][]ulid.ULID{
					0: {t0block1Ulid, t0block2Ulid, t0block3Ulid},
					1: {t0block1Ulid, t0block2Ulid, t0block3Ulid},
				},
			},
		},
		"test high level blocks generated without partitioning": {
			ranges:     []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart: 1 * time.Hour.Milliseconds(),
			rangeEnd:   9 * time.Hour.Milliseconds(),
			indexSize:  int64(50),
			indexLimit: int64(64),
			blocks: map[*metadata.Meta]int{
				blocks[t4block1Ulid]: 0, blocks[t5block1Ulid]: 0,
			},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 2,
				partitions: map[int][]ulid.ULID{
					0: {t4block1Ulid, t5block1Ulid},
					1: {t4block1Ulid, t5block1Ulid},
				},
			},
		},
		"test blocks generated by partition with series limit": {
			ranges:      []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart:  1 * time.Hour.Milliseconds(),
			rangeEnd:    9 * time.Hour.Milliseconds(),
			seriesCount: int64(14),
			seriesLimit: int64(64),
			blocks: map[*metadata.Meta]int{
				blocks[t1block1Ulid]: 0, blocks[t1block2Ulid]: 1,
				blocks[t2block1Ulid]: 0, blocks[t2block2Ulid]: 1, blocks[t2block3Ulid]: 2, blocks[t2block4Ulid]: 3,
				blocks[t3block1Ulid]: 0, blocks[t3block2Ulid]: 1, blocks[t3block3Ulid]: 2, blocks[t3block4Ulid]: 3,
				blocks[t3block5Ulid]: 4, blocks[t3block6Ulid]: 5, blocks[t3block7Ulid]: 6, blocks[t3block8Ulid]: 7},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 4,
				partitions: map[int][]ulid.ULID{
					0: {t1block1Ulid, t2block1Ulid, t3block1Ulid, t3block5Ulid},
					1: {t1block2Ulid, t2block2Ulid, t3block2Ulid, t3block6Ulid},
					2: {t1block1Ulid, t2block3Ulid, t3block3Ulid, t3block7Ulid},
					3: {t1block2Ulid, t2block4Ulid, t3block4Ulid, t3block8Ulid},
				},
			},
		},
		"test blocks generated by partition with both index and series limit set": {
			ranges:      []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart:  1 * time.Hour.Milliseconds(),
			rangeEnd:    9 * time.Hour.Milliseconds(),
			indexSize:   int64(1),
			indexLimit:  int64(64),
			seriesCount: int64(14),
			seriesLimit: int64(64),
			blocks: map[*metadata.Meta]int{
				blocks[t1block1Ulid]: 0, blocks[t1block2Ulid]: 1,
				blocks[t2block1Ulid]: 0, blocks[t2block2Ulid]: 1, blocks[t2block3Ulid]: 2, blocks[t2block4Ulid]: 3,
				blocks[t3block1Ulid]: 0, blocks[t3block2Ulid]: 1, blocks[t3block3Ulid]: 2, blocks[t3block4Ulid]: 3,
				blocks[t3block5Ulid]: 4, blocks[t3block6Ulid]: 5, blocks[t3block7Ulid]: 6, blocks[t3block8Ulid]: 7},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 4,
				partitions: map[int][]ulid.ULID{
					0: {t1block1Ulid, t2block1Ulid, t3block1Ulid, t3block5Ulid},
					1: {t1block2Ulid, t2block2Ulid, t3block2Ulid, t3block6Ulid},
					2: {t1block1Ulid, t2block3Ulid, t3block3Ulid, t3block7Ulid},
					3: {t1block2Ulid, t2block4Ulid, t3block4Ulid, t3block8Ulid},
				},
			},
		},
		"test blocks generated by partition with partition number equals to 1": {
			ranges:     []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart: 1 * time.Hour.Milliseconds(),
			rangeEnd:   9 * time.Hour.Milliseconds(),
			blocks: map[*metadata.Meta]int{
				blocks[t1block1Ulid]: 0, blocks[t1block2Ulid]: 1,
				blocks[t2block1Ulid]: 0, blocks[t2block2Ulid]: 1, blocks[t2block3Ulid]: 2, blocks[t2block4Ulid]: 3,
				blocks[t3block1Ulid]: 0, blocks[t3block2Ulid]: 1, blocks[t3block3Ulid]: 2, blocks[t3block4Ulid]: 3,
				blocks[t3block5Ulid]: 4, blocks[t3block6Ulid]: 5, blocks[t3block7Ulid]: 6, blocks[t3block8Ulid]: 7},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 1,
				partitions: map[int][]ulid.ULID{
					0: {t1block1Ulid, t1block2Ulid, t2block1Ulid, t2block2Ulid, t2block3Ulid, t2block4Ulid, t3block1Ulid,
						t3block2Ulid, t3block3Ulid, t3block4Ulid, t3block5Ulid, t3block6Ulid, t3block7Ulid, t3block8Ulid},
				},
			},
		},
		"test blocks generated by partition in random order": {
			ranges:     []time.Duration{2 * time.Hour, 6 * time.Hour},
			rangeStart: 1 * time.Hour.Milliseconds(),
			rangeEnd:   9 * time.Hour.Milliseconds(),
			indexSize:  int64(14),
			indexLimit: int64(64),
			blocks: map[*metadata.Meta]int{
				blocks[t1block2Ulid]: 1, blocks[t1block1Ulid]: 0,
				blocks[t2block4Ulid]: 3, blocks[t2block1Ulid]: 0, blocks[t2block3Ulid]: 2, blocks[t2block2Ulid]: 1,
				blocks[t3block1Ulid]: 0, blocks[t3block6Ulid]: 5, blocks[t3block3Ulid]: 2, blocks[t3block5Ulid]: 4,
				blocks[t3block2Ulid]: 1, blocks[t3block7Ulid]: 6, blocks[t3block4Ulid]: 3, blocks[t3block8Ulid]: 7},
			expected: struct {
				partitionCount int
				partitions     map[int][]ulid.ULID
			}{
				partitionCount: 4,
				partitions: map[int][]ulid.ULID{
					0: {t1block1Ulid, t2block1Ulid, t3block1Ulid, t3block5Ulid},
					1: {t1block2Ulid, t2block2Ulid, t3block2Ulid, t3block6Ulid},
					2: {t1block1Ulid, t2block3Ulid, t3block3Ulid, t3block7Ulid},
					3: {t1block2Ulid, t2block4Ulid, t3block4Ulid, t3block8Ulid},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			compactorCfg := &Config{
				BlockRanges:                    testData.ranges,
				PartitionIndexSizeLimitInBytes: testData.indexLimit,
				PartitionSeriesCountLimit:      testData.seriesLimit,
			}

			limits := &validation.Limits{}
			overrides, err := validation.NewOverrides(*limits, nil)
			require.NoError(t, err)

			ring := &RingMock{}

			bkt := &bucket.ClientMock{}

			noCompactFilter := func() map[ulid.ULID]*metadata.NoCompactMark {
				return make(map[ulid.ULID]*metadata.NoCompactMark)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := NewShuffleShardingGrouper(
				ctx,
				nil,
				objstore.WithNoopInstr(bkt),
				false, // Do not accept malformed indexes
				true,  // Enable vertical compaction
				nil,
				nil,
				nil,
				nil,
				nil,
				metadata.NoneFunc,
				*compactorCfg,
				ring,
				"test-addr",
				testCompactorID,
				overrides,
				"",
				10,
				3,
				1,
				5*time.Minute,
				nil,
				nil,
				nil,
				nil,
				noCompactFilter,
			)
			var testBlocks []*metadata.Meta
			for block, partitionID := range testData.blocks {
				block.Thanos.Files = []metadata.File{
					{RelPath: thanosblock.IndexFilename, SizeBytes: testData.indexSize},
				}
				block.Stats.NumSeries = uint64(testData.seriesCount)
				testBlocks = append(testBlocks, block)
				partitionInfo := &metadata.PartitionInfo{
					PartitionID: partitionID,
				}
				block.Thanos.PartitionInfo = partitionInfo
			}
			testGroup := blocksGroup{
				rangeStart: testData.rangeStart,
				rangeEnd:   testData.rangeEnd,
				blocks:     testBlocks,
			}
			actual, err := g.partitionBlockGroup(testGroup, uint32(0))
			require.NoError(t, err)
			require.Equal(t, testData.expected.partitionCount, actual.PartitionCount)
			require.Len(t, actual.Partitions, len(testData.expected.partitions))
			for _, actualPartition := range actual.Partitions {
				actualPartitionID := actualPartition.PartitionID
				require.ElementsMatch(t, testData.expected.partitions[actualPartitionID], actualPartition.Blocks)
			}
		})
	}
}

func TestPartitionStrategyChange_shouldUseOriginalPartitionedGroup(t *testing.T) {
	t1block1Ulid := ulid.MustNew(4, nil)
	t1block2Ulid := ulid.MustNew(5, nil)
	t2block1Ulid := ulid.MustNew(6, nil)
	t2block2Ulid := ulid.MustNew(7, nil)
	t2block3Ulid := ulid.MustNew(8, nil)
	t2block4Ulid := ulid.MustNew(9, nil)
	t3block1Ulid := ulid.MustNew(10, nil)
	t3block2Ulid := ulid.MustNew(11, nil)
	t3block3Ulid := ulid.MustNew(12, nil)
	t3block4Ulid := ulid.MustNew(13, nil)
	t3block5Ulid := ulid.MustNew(14, nil)
	t3block6Ulid := ulid.MustNew(15, nil)
	t3block7Ulid := ulid.MustNew(16, nil)
	t3block8Ulid := ulid.MustNew(17, nil)

	blocks :=
		map[ulid.ULID]*metadata.Meta{
			t1block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t1block1Ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 5 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t1block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t1block2Ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 5 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block1Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block2Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block3Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block3Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t2block4Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t2block4Ulid, MinTime: 5 * time.Hour.Milliseconds(), MaxTime: 7 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block1Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block1Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block2Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block2Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block3Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block3Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block4Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block4Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block5Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block5Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block6Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block6Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block7Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block7Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			t3block8Ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: t3block8Ulid, MinTime: 7 * time.Hour.Milliseconds(), MaxTime: 9 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
		}

	partitionedGroupID := uint32(12345)
	indexSize := int64(10)
	seriesCount := int64(10)
	testRanges := []time.Duration{2 * time.Hour, 6 * time.Hour}
	testRangeStart := 1 * time.Hour.Milliseconds()
	testRangeEnd := 9 * time.Hour.Milliseconds()
	testBlocks := map[*metadata.Meta]int{
		blocks[t1block1Ulid]: 0, blocks[t1block2Ulid]: 1,
		blocks[t2block1Ulid]: 0, blocks[t2block2Ulid]: 1, blocks[t2block3Ulid]: 2, blocks[t2block4Ulid]: 3,
		blocks[t3block1Ulid]: 0, blocks[t3block2Ulid]: 1, blocks[t3block3Ulid]: 2, blocks[t3block4Ulid]: 3,
		blocks[t3block5Ulid]: 4, blocks[t3block6Ulid]: 5, blocks[t3block7Ulid]: 6, blocks[t3block8Ulid]: 7,
	}
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	var updatedTestBlocks []*metadata.Meta
	for block, partitionID := range testBlocks {
		block.Thanos.Files = []metadata.File{
			{RelPath: thanosblock.IndexFilename, SizeBytes: indexSize},
		}
		block.Stats.NumSeries = uint64(seriesCount)
		updatedTestBlocks = append(updatedTestBlocks, block)
		partitionInfo := &metadata.PartitionInfo{
			PartitionID: partitionID,
		}
		block.Thanos.PartitionInfo = partitionInfo
	}
	testGroup := blocksGroup{
		rangeStart: testRangeStart,
		rangeEnd:   testRangeEnd,
		blocks:     updatedTestBlocks,
	}
	createGrouper := func(ctx context.Context, bkt objstore.Bucket, compactorCfg *Config) *ShuffleShardingGrouper {
		limits := &validation.Limits{}
		overrides, err := validation.NewOverrides(*limits, nil)
		require.NoError(t, err)

		ring := &RingMock{}

		noCompactFilter := func() map[ulid.ULID]*metadata.NoCompactMark {
			return make(map[ulid.ULID]*metadata.NoCompactMark)
		}

		return NewShuffleShardingGrouper(
			ctx,
			nil,
			objstore.WithNoopInstr(bkt),
			false, // Do not accept malformed indexes
			true,  // Enable vertical compaction
			nil,
			nil,
			nil,
			nil,
			nil,
			metadata.NoneFunc,
			*compactorCfg,
			ring,
			"test-addr",
			"test-compactor",
			overrides,
			"",
			10,
			3,
			1,
			5*time.Minute,
			nil,
			nil,
			nil,
			nil,
			noCompactFilter,
		)
	}

	expectedPartitions := map[int][]ulid.ULID{
		0: {t1block1Ulid, t2block1Ulid, t3block1Ulid, t3block5Ulid},
		1: {t1block2Ulid, t2block2Ulid, t3block2Ulid, t3block6Ulid},
		2: {t1block1Ulid, t2block3Ulid, t3block3Ulid, t3block7Ulid},
		3: {t1block2Ulid, t2block4Ulid, t3block4Ulid, t3block8Ulid},
	}

	// test base case
	compactorCfg1 := &Config{
		BlockRanges:                    testRanges,
		PartitionIndexSizeLimitInBytes: int64(40),
	}
	ctx, cancel := context.WithCancel(context.Background())
	grouper1 := createGrouper(ctx, bkt, compactorCfg1)
	partitionedGroup1, err := grouper1.generatePartitionBlockGroup(testGroup, partitionedGroupID)
	cancel()
	require.NoError(t, err)
	require.Equal(t, 4, partitionedGroup1.PartitionCount)
	require.Len(t, partitionedGroup1.Partitions, 4)
	partitionMap := make(map[int][]ulid.ULID)
	for _, partition := range partitionedGroup1.Partitions {
		partitionID := partition.PartitionID
		require.ElementsMatch(t, expectedPartitions[partitionID], partition.Blocks)
		partitionMap[partitionID] = partition.Blocks
	}

	// test limit increased
	compactorCfg2 := &Config{
		BlockRanges:                    testRanges,
		PartitionIndexSizeLimitInBytes: int64(80),
	}
	ctx, cancel = context.WithCancel(context.Background())
	grouper2 := createGrouper(ctx, bkt, compactorCfg2)
	partitionedGroup2, err := grouper2.generatePartitionBlockGroup(testGroup, partitionedGroupID)
	cancel()
	require.NoError(t, err)
	require.Equal(t, partitionedGroup1.PartitionCount, partitionedGroup2.PartitionCount)
	require.Len(t, partitionedGroup2.Partitions, len(partitionedGroup1.Partitions))
	for _, partition := range partitionedGroup2.Partitions {
		partitionID := partition.PartitionID
		require.ElementsMatch(t, partitionMap[partitionID], partition.Blocks)
	}

	// test limit decreased
	compactorCfg3 := &Config{
		BlockRanges:                    testRanges,
		PartitionIndexSizeLimitInBytes: int64(20),
	}
	ctx, cancel = context.WithCancel(context.Background())
	grouper3 := createGrouper(ctx, bkt, compactorCfg3)
	partitionedGroup3, err := grouper3.generatePartitionBlockGroup(testGroup, partitionedGroupID)
	cancel()
	require.NoError(t, err)
	require.Equal(t, partitionedGroup1.PartitionCount, partitionedGroup3.PartitionCount)
	require.Len(t, partitionedGroup3.Partitions, len(partitionedGroup1.Partitions))
	for _, partition := range partitionedGroup3.Partitions {
		partitionID := partition.PartitionID
		require.ElementsMatch(t, partitionMap[partitionID], partition.Blocks)
	}
}

type RingMock struct {
	mock.Mock
}

func (r *RingMock) Collect(ch chan<- prometheus.Metric) {}

func (r *RingMock) Describe(ch chan<- *prometheus.Desc) {}

func (r *RingMock) Get(key uint32, op ring.Operation, bufDescs []ring.InstanceDesc, bufHosts, bufZones []string) (ring.ReplicationSet, error) {
	args := r.Called(key, op, bufDescs, bufHosts, bufZones)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (r *RingMock) GetAllHealthy(op ring.Operation) (ring.ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (r *RingMock) GetReplicationSetForOperation(op ring.Operation) (ring.ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (r *RingMock) ReplicationFactor() int {
	return 0
}

func (r *RingMock) InstancesCount() int {
	return 0
}

func (r *RingMock) ShuffleShard(identifier string, size int) ring.ReadRing {
	args := r.Called(identifier, size)
	return args.Get(0).(ring.ReadRing)
}

func (r *RingMock) GetInstanceState(instanceID string) (ring.InstanceState, error) {
	args := r.Called(instanceID)
	return args.Get(0).(ring.InstanceState), args.Error(1)
}

func (r *RingMock) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ring.ReadRing {
	args := r.Called(identifier, size, lookbackPeriod, now)
	return args.Get(0).(ring.ReadRing)
}

func (r *RingMock) HasInstance(instanceID string) bool {
	return true
}

func (r *RingMock) CleanupShuffleShardCache(identifier string) {}
