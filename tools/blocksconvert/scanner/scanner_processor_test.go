package scanner

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

func TestProcessorError(t *testing.T) {
	entries := map[string][]blocksconvert.PlanEntry{}
	resultFn := func(dir string, file string, entry blocksconvert.PlanEntry, header func() blocksconvert.PlanEntry) error {
		full := path.Join(dir, file)

		entries[full] = append(entries[full], entry)
		return nil
	}

	p := newProcessor("output", resultFn, nil, nil, prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"type"}), prometheus.NewCounter(prometheus.CounterOpts{}))

	// Day 18500
	now := time.Unix(1598456178, 0)
	startOfDay := model.TimeFromUnixNano(now.Truncate(24 * time.Hour).UnixNano())

	pc := chunk.PeriodConfig{
		From:       chunk.DayTime{Time: startOfDay},
		IndexType:  "bigtable",
		ObjectType: "gcs",
		Schema:     "v9",
		IndexTables: chunk.PeriodicTableConfig{
			Prefix: "index_",
			Period: 7 * 24 * time.Hour,
		},
		ChunkTables: chunk.PeriodicTableConfig{
			Prefix: "chunks_",
			Period: 7 * 24 * time.Hour,
		},
	}
	schema, err := pc.CreateSchema()
	require.NoError(t, err)

	sschema := schema.(chunk.SeriesStoreSchema)

	// Label write entries are ignored by the processor
	{
		_, ies, err := sschema.GetCacheKeysAndLabelWriteEntries(startOfDay.Add(1*time.Hour), startOfDay.Add(2*time.Hour), "testUser", "test_metric", labels.Labels{
			{Name: "__name__", Value: "test_metric"},
		}, "chunkID")
		require.NoError(t, err)
		for _, es := range ies {
			passEntriesToProcessor(t, p, es)
		}
	}

	// Processor expects all entries for same series to arrive in sequence, before receiving different series. BigTable reader guarantees that.
	{
		es, err := sschema.GetChunkWriteEntries(startOfDay.Add(2*time.Hour), startOfDay.Add(3*time.Hour), "testUser", "test_metric", labels.Labels{
			{Name: "__name__", Value: "test_metric"},
		}, "chunkID_1")
		require.NoError(t, err)
		passEntriesToProcessor(t, p, es)
	}

	{
		es, err := sschema.GetChunkWriteEntries(startOfDay.Add(23*time.Hour), startOfDay.Add(25*time.Hour), "testUser", "test_metric", labels.Labels{
			{Name: "__name__", Value: "test_metric"},
		}, "chunkID_2")
		require.NoError(t, err)
		passEntriesToProcessor(t, p, es)
	}

	{
		es, err := sschema.GetChunkWriteEntries(startOfDay.Add(5*time.Hour), startOfDay.Add(6*time.Hour), "testUser", "different_metric", labels.Labels{
			{Name: "__name__", Value: "different_metric"},
		}, "chunkID_5")
		require.NoError(t, err)
		passEntriesToProcessor(t, p, es)
	}

	require.NoError(t, p.Flush())

	// Now let's compare what we have received.
	require.Equal(t, map[string][]blocksconvert.PlanEntry{
		"output/testUser/18500.plan": {
			{
				SeriesID: "eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
				Chunks:   []string{"chunkID_1", "chunkID_2"},
			},
			{
				SeriesID: "+afMmul/w5PDAAWGPd7y8+xyBq5IN+Q2/ZnPnrMEI+k",
				Chunks:   []string{"chunkID_5"},
			},
		},

		"output/testUser/18501.plan": {
			{
				SeriesID: "eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
				Chunks:   []string{"chunkID_2"},
			},
		},
	}, entries)
}

func passEntriesToProcessor(t *testing.T, p *processor, es []chunk.IndexEntry) {
	for _, ie := range es {
		fmt.Printf("%q %q %q\n", ie.HashValue, ie.RangeValue, ie.Value)

		require.NoError(t, p.ProcessIndexEntry(ie))
	}
}
