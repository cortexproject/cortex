package chunk

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
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
	ctx := user.WithID(context.Background(), "0")
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
