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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/test"
	"github.com/weaveworks/common/user"
)

// newTestStore creates a new Store for testing.
func newTestChunkStore(t *testing.T, cfg StoreConfig) *Store {
	storage := NewMockStorage()
	schemaCfg := SchemaConfig{}
	tableManager, err := NewTableManager(schemaCfg, storage)
	require.NoError(t, err)
	err = tableManager.syncTables(context.Background())
	require.NoError(t, err)
	store, err := NewStore(cfg, schemaCfg, storage)
	require.NoError(t, err)
	return store
}

func createSampleStreamFrom(chunk Chunk) (*model.SampleStream, error) {
	samples, err := chunk.Samples()
	if err != nil {
		return nil, err
	}
	return &model.SampleStream{
		Metric: chunk.Metric,
		Values: samples,
	}, nil
}

// Allow sorting of local.SeriesIterator by fingerprint (for comparisation tests)
type ByFingerprint model.Matrix

func (bfp ByFingerprint) Len() int {
	return len(bfp)
}
func (bfp ByFingerprint) Swap(i, j int) {
	bfp[i], bfp[j] = bfp[j], bfp[i]
}
func (bfp ByFingerprint) Less(i, j int) bool {
	return bfp[i].Metric.Fingerprint() < bfp[j].Metric.Fingerprint()
}

// TODO(prom2): reintroduce tests that were part of TestChunkStore_Get_lazy
// TestChunkStore_Get tests results are returned correctly depending on the type of query
func TestChunkStore_Get(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), userID)
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

	foo1SampleStream1, err := createSampleStreamFrom(foo1Chunk1)
	require.NoError(t, err)
	foo1SampleStream2, err := createSampleStreamFrom(foo1Chunk2)
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

	nameMatcher := mustNewLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo1")

	for _, tc := range []struct {
		query    string
		expect   model.Matrix
		matchers []*labels.Matcher
	}{
		{
			`foo1`,
			model.Matrix{foo1SampleStream1, foo1SampleStream2},
			[]*labels.Matcher{nameMatcher},
		},
		{
			`foo1{flip=""}`,
			model.Matrix{foo1SampleStream2},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchEqual, "flip", "")},
		},
		{
			`foo1{bar="baz"}`,
			model.Matrix{foo1SampleStream1},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchEqual, "bar", "baz")},
		},
		{
			`foo1{bar="beep"}`,
			model.Matrix{foo1SampleStream2},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchEqual, "bar", "beep")},
		},
		{
			`foo1{toms="code"}`,
			model.Matrix{foo1SampleStream1, foo1SampleStream2},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchEqual, "toms", "code")},
		},
		{
			`foo1{bar!="baz"}`,
			model.Matrix{foo1SampleStream2},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchNotEqual, "bar", "baz")},
		},
		{
			`foo1{bar=~"beep|baz"}`,
			model.Matrix{foo1SampleStream1, foo1SampleStream2},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchRegexp, "bar", "beep|baz")},
		},
		{
			`foo1{toms="code", bar=~"beep|baz"}`,
			model.Matrix{foo1SampleStream1, foo1SampleStream2},
			[]*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchEqual, "toms", "code"), mustNewLabelMatcher(labels.MatchRegexp, "bar", "beep|baz")},
		},
		{
			`foo1{toms="code", bar="baz"}`,
			model.Matrix{foo1SampleStream1}, []*labels.Matcher{nameMatcher, mustNewLabelMatcher(labels.MatchEqual, "toms", "code"), mustNewLabelMatcher(labels.MatchEqual, "bar", "baz")},
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

				// Query with ordinary time-range
				iterators1, err := store.Get(ctx, now.Add(-time.Hour), now, tc.matchers...)
				require.NoError(t, err)

				sort.Sort(ByFingerprint(iterators1))
				if !reflect.DeepEqual(tc.expect, iterators1) {
					t.Fatalf("%s: wrong chunks - %s", tc.query, test.Diff(tc.expect, iterators1))
				}

				// Pushing end of time-range into future should yield exact same resultset
				iterators2, err := store.Get(ctx, now.Add(-time.Hour), now.Add(time.Hour*24*30), tc.matchers...)
				require.NoError(t, err)

				sort.Sort(ByFingerprint(iterators2))
				if !reflect.DeepEqual(tc.expect, iterators2) {
					t.Fatalf("%s: wrong chunks - %s", tc.query, test.Diff(tc.expect, iterators2))
				}

				// Query with both begin & end of time-range in future should yield empty resultset
				iterators3, err := store.Get(ctx, now.Add(time.Hour), now.Add(time.Hour*2), tc.matchers...)
				require.NoError(t, err)
				if len(iterators3) != 0 {
					t.Fatalf("%s: future query should yield empty resultset ... actually got %v chunks: %#v",
						tc.query, len(iterators3), iterators3)
				}
			})
		}
	}
}

// TestChunkStore_getMetricNameChunks tests if chunks are fetched correctly when we have the metric name
func TestChunkStore_getMetricNameChunks(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), userID)
	now := model.Now()
	metricName := "foo"
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

	for _, tc := range []struct {
		query    string
		expect   []Chunk
		matchers []*labels.Matcher
	}{
		{
			`foo`,
			[]Chunk{chunk1, chunk2},
			[]*labels.Matcher{},
		},
		{
			`foo{flip=""}`,
			[]Chunk{chunk2},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "flip", "")},
		},
		{
			`foo{bar="baz"}`,
			[]Chunk{chunk1},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "bar", "baz")},
		},
		{
			`foo{bar="beep"}`,
			[]Chunk{chunk2},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "bar", "beep")},
		},
		{
			`foo{toms="code"}`,
			[]Chunk{chunk1, chunk2},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "toms", "code")},
		},
		{
			`foo{bar!="baz"}`,
			[]Chunk{chunk2},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchNotEqual, "bar", "baz")},
		},
		{
			`foo{bar=~"beep|baz"}`,
			[]Chunk{chunk1, chunk2},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchRegexp, "bar", "beep|baz")},
		},
		{
			`foo{toms="code", bar=~"beep|baz"}`,
			[]Chunk{chunk1, chunk2},
			[]*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "toms", "code"), mustNewLabelMatcher(labels.MatchRegexp, "bar", "beep|baz")},
		},
		{
			`foo{toms="code", bar="baz"}`,
			[]Chunk{chunk1}, []*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "toms", "code"), mustNewLabelMatcher(labels.MatchEqual, "bar", "baz")},
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

				chunks, err := store.getMetricNameChunks(ctx, now.Add(-time.Hour), now, tc.matchers, metricName)
				require.NoError(t, err)

				if !reflect.DeepEqual(tc.expect, chunks) {
					t.Fatalf("%s: wrong chunks - %s", tc.query, test.Diff(tc.expect, chunks))
				}
			})
		}
	}
}

func mustNewLabelMatcher(matchType labels.MatchType, name string, value string) *labels.Matcher {
	matcher, err := labels.NewMatcher(matchType, name, value)
	if err != nil {
		panic(err)
	}
	return matcher
}

func TestChunkStoreRandom(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), userID)
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

		metricNameLabel := mustNewLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo")
		matchers := []*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "bar", "baz")}

		for _, s := range schemas {
			chunks, err := s.store.getMetricNameChunks(ctx, startTime, endTime,
				matchers,
				metricNameLabel.Value,
			)
			require.NoError(t, err)

			// We need to check that each chunk is in the time range
			for _, chunk := range chunks {
				assert.False(t, chunk.From.After(endTime))
				assert.False(t, chunk.Through.Before(startTime))
				samples, err := chunk.Samples()
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
	ctx := user.InjectOrgID(context.Background(), userID)
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

		metricNameLabel := mustNewLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo")
		matchers := []*labels.Matcher{mustNewLabelMatcher(labels.MatchEqual, "bar", "baz")}

		chunks, err := store.getMetricNameChunks(ctx, startTime, endTime,
			matchers,
			metricNameLabel.Value,
		)
		if err != nil {
			t.Fatal(t, err)
		}

		// We need to check that each chunk is in the time range
		for _, chunk := range chunks {
			assert.False(t, chunk.From.After(endTime))
			assert.False(t, chunk.Through.Before(startTime))
			samples, err := chunk.Samples()
			assert.NoError(t, err)
			assert.Equal(t, 1, len(samples))
		}

		// And check we got all the chunks we want
		numChunks := 24 - (start / chunkLen) + 1
		assert.Equal(t, int(numChunks), len(chunks))
	}
}
