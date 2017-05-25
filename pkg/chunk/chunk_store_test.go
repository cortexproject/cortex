package chunk

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/test"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/util"
)

// newTestStore creates a new Store for testing.
func newTestChunkStore(t *testing.T, cfg StoreConfig) *Store {
	storage := NewMockStorage()
	tableManager, err := NewTableManager(TableManagerConfig{}, storage)
	require.NoError(t, err)
	err = tableManager.syncTables(context.Background())
	require.NoError(t, err)
	store, err := NewStore(cfg, storage)
	require.NoError(t, err)
	return store
}

func createSampleStreamIteratorFrom(chunk Chunk) (local.SeriesIterator, error) {
	samples, err := chunk.samples()
	if err != nil {
		return nil, err
	}
	return util.NewSampleStreamIterator(&model.SampleStream{
		Metric: chunk.Metric,
		Values: samples,
	}), nil
}

// Allow sorting of local.SeriesIterator by fingerprint (for comparisation tests)
type ByFingerprint []local.SeriesIterator

func (s ByFingerprint) Len() int {
	return len(s)
}
func (s ByFingerprint) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByFingerprint) Less(i, j int) bool {
	return s[i].Metric().Metric.Fingerprint() < s[j].Metric().Metric.Fingerprint()
}

// TestChunkStore_Get tests iterators are returned correctly depending on the type of query
func TestChunkStore_Get_concrete(t *testing.T) {
	ctx := user.Inject(context.Background(), userID)
	now := model.Now()

	// foo chunks (used for fuzzy lazy iterator tests)
	foo1Metric1 := model.Metric{
		model.MetricNameLabel: "foo1",
		"bar":  "baz",
		"toms": "code",
		"flip": "flop",
	}
	foo1Metric2 := model.Metric{
		model.MetricNameLabel: "foo1",
		"bar":  "beep",
		"toms": "code",
	}

	foo1Chunk1 := dummyChunkFor(foo1Metric1)
	foo1Chunk2 := dummyChunkFor(foo1Metric2)

	foo1Iterator1, err := createSampleStreamIteratorFrom(foo1Chunk1)
	require.NoError(t, err)
	foo1Iterator2, err := createSampleStreamIteratorFrom(foo1Chunk2)
	require.NoError(t, err)

	schemas := []struct {
		name string
		fn   func(cfg SchemaConfig) Schema
	}{
		{"v1 schema", v1Schema},
		{"v2 schema", v2Schema},
		{"v3 schema", v3Schema},
		{"v4 schema", v4Schema},
		{"v5 schema", v5Schema},
		{"v6 schema", v6Schema},
		{"v7 schema", v7Schema},
		{"v8 schema", v8Schema},
	}

	nameMatcher := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo1")

	for _, tc := range []struct {
		query    string
		expect   []local.SeriesIterator
		matchers []*metric.LabelMatcher
	}{
		{
			`foo1`,
			[]local.SeriesIterator{foo1Iterator1, foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher},
		},
		{
			`foo1{flip=""}`,
			[]local.SeriesIterator{foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "flip", "")},
		},
		{
			`foo1{bar="baz"}`,
			[]local.SeriesIterator{foo1Iterator1},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
		{
			`foo1{bar="beep"}`,
			[]local.SeriesIterator{foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "beep")},
		},
		{
			`foo1{toms="code"}`,
			[]local.SeriesIterator{foo1Iterator1, foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code")},
		},
		{
			`foo1{bar!="baz"}`,
			[]local.SeriesIterator{foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.NotEqual, "bar", "baz")},
		},
		{
			`foo1{bar=~"beep|baz"}`,
			[]local.SeriesIterator{foo1Iterator1, foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			`foo1{toms="code", bar=~"beep|baz"}`,
			[]local.SeriesIterator{foo1Iterator1, foo1Iterator2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			`foo1{toms="code", bar="baz"}`,
			[]local.SeriesIterator{foo1Iterator1}, []*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
	} {
		for _, schema := range schemas {
			t.Run(fmt.Sprintf("%s / %s", tc.query, schema.name), func(t *testing.T) {
				log.Infoln("========= Running query", tc.query, "with schema", schema.name)
				store := newTestChunkStore(t, StoreConfig{
					schemaFactory: schema.fn,
				})

				if err := store.Put(ctx, []Chunk{
					foo1Chunk1,
					foo1Chunk2,
				}); err != nil {
					t.Fatal(err)
				}

				iterators, err := store.Get(ctx, now.Add(-time.Hour), now, tc.matchers...)
				require.NoError(t, err)

				sort.Sort(ByFingerprint(iterators))
				if !reflect.DeepEqual(tc.expect, iterators) {
					t.Fatalf("%s: wrong chunks - %s", tc.query, test.Diff(tc.expect, iterators))
				}
			})
		}
	}
}

// TestChunkStore_Get tests iterators are returned correctly depending on the type of query
func TestChunkStore_Get_lazy(t *testing.T) {
	ctx := user.Inject(context.Background(), userID)
	now := model.Now()

	// foo chunks (used for fuzzy lazy iterator tests)
	foo1Metric1 := model.Metric{
		model.MetricNameLabel: "foo1",
		"bar":  "baz",
		"toms": "code",
		"flip": "flop",
	}
	foo1Metric2 := model.Metric{
		model.MetricNameLabel: "foo1",
		"bar":  "beep",
		"toms": "code",
	}
	foo2Metric := model.Metric{
		model.MetricNameLabel: "foo2",
		"bar":  "beep",
		"toms": "code",
	}
	foo3Metric := model.Metric{
		model.MetricNameLabel: "foo3",
		"bar":  "beep",
		"toms": "code",
	}

	foo1Chunk1 := dummyChunkFor(foo1Metric1)
	foo1Chunk2 := dummyChunkFor(foo1Metric2)
	foo2Chunk := dummyChunkFor(foo2Metric)
	foo3Chunk := dummyChunkFor(foo3Metric)

	foo1LazyIterator1 := util.NewLazySeriesIterator(foo1Metric1)
	foo1LazyIterator2 := util.NewLazySeriesIterator(foo1Metric2)
	foo2LazyIterator := util.NewLazySeriesIterator(foo2Metric)
	foo3LazyIterator := util.NewLazySeriesIterator(foo3Metric)

	schemas := []struct {
		name string
		fn   func(cfg SchemaConfig) Schema
	}{
		{"v8 schema", v8Schema},
	}

	for _, tc := range []struct {
		query    string
		expect   []local.SeriesIterator
		matchers []*metric.LabelMatcher
	}{
		// When name matcher is used without Equal, start matching all metric names
		// however still filter out metric names which do not match query
		{
			`{__name__!="foo1"}`,
			[]local.SeriesIterator{foo3LazyIterator, foo2LazyIterator},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.NotEqual, model.MetricNameLabel, "foo1")},
		},
		{
			`{__name__=~"foo1|foo2"}`,
			[]local.SeriesIterator{foo1LazyIterator1, foo2LazyIterator, foo1LazyIterator2},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.RegexMatch, model.MetricNameLabel, "foo1|foo2")},
		},

		// No metric names
		{
			`{bar="baz"}`,
			[]local.SeriesIterator{foo1LazyIterator1},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
		{
			`{bar="beep"}`,
			[]local.SeriesIterator{foo3LazyIterator, foo2LazyIterator, foo1LazyIterator2}, // doesn't match foo1 metric 1
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "bar", "beep")},
		},
		{
			`{flip=""}`,
			[]local.SeriesIterator{foo3LazyIterator, foo2LazyIterator, foo1LazyIterator2}, // doesn't match foo1 chunk1 as it has a flip value
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "flip", "")},
		},
		{
			`{bar!="beep"}`,
			[]local.SeriesIterator{foo1LazyIterator1},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.NotEqual, "bar", "beep")},
		},
		{
			`{bar=~"beep|baz"}`,
			[]local.SeriesIterator{foo3LazyIterator, foo1LazyIterator1, foo2LazyIterator, foo1LazyIterator2},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			`{toms="code", bar=~"beep|baz"}`,
			[]local.SeriesIterator{foo3LazyIterator, foo1LazyIterator1, foo2LazyIterator, foo1LazyIterator2},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			`{toms="code", bar="baz"}`,
			[]local.SeriesIterator{foo1LazyIterator1},
			[]*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
	} {
		for _, schema := range schemas {
			t.Run(fmt.Sprintf("%s / %s", tc.query, schema.name), func(t *testing.T) {
				log.Infoln("========= Running query", tc.query, "with schema", schema.name)
				store := newTestChunkStore(t, StoreConfig{
					schemaFactory: schema.fn,
				})

				if err := store.Put(ctx, []Chunk{
					foo1Chunk1,
					foo1Chunk2,
					foo2Chunk,
					foo3Chunk,
				}); err != nil {
					t.Fatal(err)
				}

				iterators, err := store.Get(ctx, now.Add(-time.Hour), now, tc.matchers...)
				require.NoError(t, err)

				sort.Sort(ByFingerprint(iterators))
				if !reflect.DeepEqual(tc.expect, iterators) {
					t.Fatalf("%s: wrong iterators - %s", tc.query, test.Diff(tc.expect, iterators))
				}
			})
		}
	}
}

// TestChunkStore_getMetricNameChunks tests if chunks are fetched correctly when we have the metric name
func TestChunkStore_getMetricNameChunks(t *testing.T) {
	ctx := user.Inject(context.Background(), userID)
	now := model.Now()
	chunk1 := dummyChunkFor(model.Metric{
		model.MetricNameLabel: "foo",
		"bar":  "baz",
		"toms": "code",
		"flip": "flop",
	})
	chunk2 := dummyChunkFor(model.Metric{
		model.MetricNameLabel: "foo",
		"bar":  "beep",
		"toms": "code",
	})

	schemas := []struct {
		name string
		fn   func(cfg SchemaConfig) Schema
	}{
		{"v1 schema", v1Schema},
		{"v2 schema", v2Schema},
		{"v3 schema", v3Schema},
		{"v4 schema", v4Schema},
		{"v5 schema", v5Schema},
		{"v6 schema", v6Schema},
		{"v7 schema", v7Schema},
		{"v8 schema", v8Schema},
	}

	nameMatcher := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo")

	for _, tc := range []struct {
		query    string
		expect   []Chunk
		matchers []*metric.LabelMatcher
	}{
		{
			`foo`,
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher},
		},
		{
			`foo{flip=""}`,
			[]Chunk{chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "flip", "")},
		},
		{
			`foo{bar="baz"}`,
			[]Chunk{chunk1},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
		{
			`foo{bar="beep"}`,
			[]Chunk{chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "beep")},
		},
		{
			`foo{toms="code"}`,
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code")},
		},
		{
			`foo{bar!="baz"}`,
			[]Chunk{chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.NotEqual, "bar", "baz")},
		},
		{
			`foo{bar=~"beep|baz"}`,
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			`foo{toms="code", bar=~"beep|baz"}`,
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			`foo{toms="code", bar="baz"}`,
			[]Chunk{chunk1}, []*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
	} {
		for _, schema := range schemas {
			t.Run(fmt.Sprintf("%s / %s", tc.query, schema.name), func(t *testing.T) {
				log.Infoln("========= Running query", tc.query, "with schema", schema.name)
				store := newTestChunkStore(t, StoreConfig{
					schemaFactory: schema.fn,
				})

				if err := store.Put(ctx, []Chunk{chunk1, chunk2}); err != nil {
					t.Fatal(err)
				}

				filters := []*metric.LabelMatcher{}
				metricName := nameMatcher.Value
				chunks, err := store.getMetricNameChunks(ctx, now.Add(-time.Hour), now, tc.matchers, filters, metricName)
				require.NoError(t, err)

				if !reflect.DeepEqual(tc.expect, chunks) {
					t.Fatalf("%s: wrong chunks - %s", tc.query, test.Diff(tc.expect, chunks))
				}
			})
		}
	}
}

func mustNewLabelMatcher(matchType metric.MatchType, name model.LabelName, value model.LabelValue) *metric.LabelMatcher {
	matcher, err := metric.NewLabelMatcher(matchType, name, value)
	if err != nil {
		panic(err)
	}
	return matcher
}

func TestChunkStoreRandom(t *testing.T) {
	ctx := user.Inject(context.Background(), userID)
	schemas := []struct {
		name  string
		fn    func(cfg SchemaConfig) Schema
		store *Store
	}{
		{name: "v1 schema", fn: v1Schema},
		{name: "v2 schema", fn: v2Schema},
		{name: "v3 schema", fn: v3Schema},
		{name: "v4 schema", fn: v4Schema},
		{name: "v5 schema", fn: v5Schema},
		{name: "v6 schema", fn: v6Schema},
		{name: "v7 schema", fn: v7Schema},
		{name: "v8 schema", fn: v8Schema},
	}

	for i := range schemas {
		schemas[i].store = newTestChunkStore(t, StoreConfig{
			schemaFactory: schemas[i].fn,
		})
	}

	// put 100 chunks from 0 to 99
	const chunkLen = 13 * 3600 // in seconds
	for i := 0; i < 100; i++ {
		ts := model.TimeFromUnix(int64(i * chunkLen))
		chunks, _ := chunk.New().Add(model.SamplePair{
			Timestamp: ts,
			Value:     model.SampleValue(float64(i)),
		})
		chunk := NewChunk(
			userID,
			model.Fingerprint(1),
			model.Metric{
				model.MetricNameLabel: "foo",
				"bar": "baz",
			},
			chunks[0],
			ts,
			ts.Add(chunkLen*time.Second),
		)
		for _, s := range schemas {
			err := s.store.Put(ctx, []Chunk{chunk})
			require.NoError(t, err)
		}
	}

	// pick two random numbers and do a query
	for i := 0; i < 100; i++ {
		start := rand.Int63n(100 * chunkLen)
		end := start + rand.Int63n((100*chunkLen)-start)
		assert.True(t, start < end)

		startTime := model.TimeFromUnix(start)
		endTime := model.TimeFromUnix(end)

		metricNameLabel := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo")
		matchers := []*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "bar", "baz")}
		filters := []*metric.LabelMatcher{}

		for _, s := range schemas {
			chunks, err := s.store.getMetricNameChunks(ctx, startTime, endTime,
				matchers,
				filters,
				metricNameLabel.Value,
			)
			require.NoError(t, err)

			// We need to check that each chunk is in the time range
			for _, chunk := range chunks {
				assert.False(t, chunk.From.After(endTime))
				assert.False(t, chunk.Through.Before(startTime))
				samples, err := chunk.samples()
				assert.NoError(t, err)
				assert.Equal(t, 1, len(samples))
				// TODO verify chunk contents
			}

			// And check we got all the chunks we want
			numChunks := (end / chunkLen) - (start / chunkLen) + 1
			assert.Equal(t, int(numChunks), len(chunks), s.name)
		}
	}
}

func TestChunkStoreLeastRead(t *testing.T) {
	// Test we don't read too much from the index
	ctx := user.Inject(context.Background(), userID)
	store := newTestChunkStore(t, StoreConfig{
		schemaFactory: v6Schema,
	})

	// Put 24 chunks 1hr chunks in the store
	const chunkLen = 60 // in seconds
	for i := 0; i < 24; i++ {
		ts := model.TimeFromUnix(int64(i * chunkLen))
		chunks, _ := chunk.New().Add(model.SamplePair{
			Timestamp: ts,
			Value:     model.SampleValue(float64(i)),
		})
		chunk := NewChunk(
			userID,
			model.Fingerprint(1),
			model.Metric{
				model.MetricNameLabel: "foo",
				"bar": "baz",
			},
			chunks[0],
			ts,
			ts.Add(chunkLen*time.Second),
		)
		log.Infof("Loop %d", i)
		err := store.Put(ctx, []Chunk{chunk})
		require.NoError(t, err)
	}

	// pick a random numbers and do a query to end of row
	for i := 1; i < 24; i++ {
		start := int64(i * chunkLen)
		end := int64(24 * chunkLen)
		assert.True(t, start <= end)

		startTime := model.TimeFromUnix(start)
		endTime := model.TimeFromUnix(end)

		metricNameLabel := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo")
		matchers := []*metric.LabelMatcher{mustNewLabelMatcher(metric.Equal, "bar", "baz")}
		filters := []*metric.LabelMatcher{}

		chunks, err := store.getMetricNameChunks(ctx, startTime, endTime,
			matchers,
			filters,
			metricNameLabel.Value,
		)
		if err != nil {
			t.Fatal(t, err)
		}

		// We need to check that each chunk is in the time range
		for _, chunk := range chunks {
			assert.False(t, chunk.From.After(endTime))
			assert.False(t, chunk.Through.Before(startTime))
			samples, err := chunk.samples()
			assert.NoError(t, err)
			assert.Equal(t, 1, len(samples))
		}

		// And check we got all the chunks we want
		numChunks := 24 - (start / chunkLen) + 1
		assert.Equal(t, int(numChunks), len(chunks))
	}
}
