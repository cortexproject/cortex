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

				// Zero out the checksums, as the inputs above didn't have the checksums calculated
				for i := range chunks {
					chunks[i].Checksum = 0
					chunks[i].ChecksumSet = false
				}

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

			// And check we got all the chunks we want
			numChunks := (end / chunkLen) - (start / chunkLen) + 1
			assert.Equal(t, int(numChunks), len(chunks), s.name)
		}
	}
}
