package chunk

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/test"
	"github.com/weaveworks/common/user"
)

func setupDynamodb(t *testing.T, dynamoDB StorageClient) {
	tableManager, err := NewDynamoTableManager(TableManagerConfig{
		mockDynamoDB: dynamoDB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tableManager.syncTables(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChunkStore(t *testing.T) {
	ctx := user.Inject(context.Background(), "0")
	now := model.Now()
	chunks, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk1 := NewChunk(
		model.Fingerprint(1),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "baz",
			"toms": "code",
			"flip": "flop",
		},
		chunks[0],
		now.Add(-time.Hour),
		now,
	)
	chunk2 := NewChunk(
		model.Fingerprint(2),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "beep",
			"toms": "code",
		},
		chunks[0],
		now.Add(-time.Hour),
		now,
	)

	schemas := []struct {
		name string
		fn   func(cfg SchemaConfig) Schema
	}{
		{"v1 schema", v1Schema},
		{"v2 schema", v2Schema},
		{"v3 schema", v3Schema},
		{"v4 schema", v4Schema},
		{"v5 schema", v5Schema},
	}

	nameMatcher := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo")

	for _, tc := range []struct {
		name     string
		expect   []Chunk
		matchers []*metric.LabelMatcher
	}{
		{
			"Just name label",
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher},
		},
		{
			"Empty matcher",
			[]Chunk{chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "flip", "")},
		},
		{
			"Equal bar=baz",
			[]Chunk{chunk1},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
		{
			"Equal bar=beep",
			[]Chunk{chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "beep")},
		},
		{
			"Equal toms=code",
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code")},
		},
		{
			"Not equal",
			[]Chunk{chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.NotEqual, "bar", "baz")},
		},
		{
			"Regex match",
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			"Multiple matchers",
			[]Chunk{chunk1, chunk2},
			[]*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz")},
		},
		{
			"Multiple matchers II",
			[]Chunk{chunk1}, []*metric.LabelMatcher{nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.Equal, "bar", "baz")},
		},
	} {
		for _, schema := range schemas {
			t.Run(fmt.Sprintf("%s/%s", tc.name, schema.name), func(t *testing.T) {
				log.Infoln("========= Running test", tc.name, "with schema", schema.name)
				dynamoDB := NewMockStorage()
				setupDynamodb(t, dynamoDB)
				store, err := NewStore(StoreConfig{
					mockDynamoDB:  dynamoDB,
					mockS3:        NewMockS3(),
					schemaFactory: schema.fn,
				})
				if err != nil {
					t.Fatal(err)
				}

				if err := store.Put(ctx, []Chunk{chunk1, chunk2}); err != nil {
					t.Fatal(err)
				}

				chunks, err := store.Get(ctx, now.Add(-time.Hour), now, tc.matchers...)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(tc.expect, chunks) {
					t.Fatalf("%s: wrong chunks - %s", tc.name, test.Diff(tc.expect, chunks))
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
	ctx := user.Inject(context.Background(), "0")
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
	}

	for i := range schemas {
		dynamoDB := NewMockStorage()
		setupDynamodb(t, dynamoDB)
		store, err := NewStore(StoreConfig{
			mockDynamoDB:  dynamoDB,
			mockS3:        NewMockS3(),
			schemaFactory: schemas[i].fn,
		})
		if err != nil {
			t.Fatal(err)
		}
		schemas[i].store = store
	}

	// put 100 chunks from 0 to 99
	const chunkLen = 13 * 3600 // in seconds
	for i := 0; i < 100; i++ {
		ts := model.TimeFromUnix(int64(i * chunkLen))
		chunks, _ := chunk.New().Add(model.SamplePair{Timestamp: ts, Value: 0})
		chunk := NewChunk(
			model.Fingerprint(1),
			model.Metric{
				model.MetricNameLabel: "foo",
				"bar": "baz",
			},
			chunks[0],
			ts,
			ts.Add(chunkLen),
		)
		for _, s := range schemas {
			if err := s.store.Put(ctx, []Chunk{chunk}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// pick two random numbers and do a query
	for i := 0; i < 100; i++ {
		start := rand.Int63n(100 * chunkLen)
		end := start + rand.Int63n((100*chunkLen)-start)
		assert.True(t, start < end)

		startTime := model.TimeFromUnix(start)
		endTime := model.TimeFromUnix(end)

		for _, s := range schemas {
			chunks, err := s.store.Get(ctx, startTime, endTime,
				mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo"),
				mustNewLabelMatcher(metric.Equal, "bar", "baz"),
			)
			if err != nil {
				t.Fatal(err)
			}

			// We need to check that each chunk is in the time range
			for _, chunk := range chunks {
				assert.False(t, chunk.From.After(endTime))
				assert.False(t, chunk.Through.Before(startTime))
			}

			// And check we go all the chunks we want
			numChunks := (end / chunkLen) - (start / chunkLen)
			assert.Equal(t, int(numChunks), len(chunks), s.name)
		}
	}
}
