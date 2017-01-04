package chunk

import (
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/user"
)

func init() {
	spew.Config.SortKeys = true // :\
}

func TestChunkStoreUnprocessed(t *testing.T) {
	store, err := NewAWSStore(StoreConfig{
		dynamodb: NewMockDynamoDB(2, 2),
		s3:       NewMockS3(),
	})
	if err != nil {
		t.Fatal(err)
	}
	store.CreateTables()
	defer store.Stop()

	ctx := user.WithID(context.Background(), "0")
	now := model.Now()
	chunks, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := NewChunk(
		model.Fingerprint(1),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "baz",
			"toms": "code",
		},
		&chunk.Desc{
			ChunkFirstTime: now.Add(-time.Hour),
			ChunkLastTime:  now,
			C:              chunks[0],
		},
	)
	want := []Chunk{chunk}
	if err := store.Put(ctx, want); err != nil {
		t.Fatal(err)
	}
	have, err := store.Get(ctx, now.Add(-time.Hour), now, mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatalf("wrong chunks - %s", diff(want, have))
	}
}

func TestChunkStore(t *testing.T) {
	store, err := NewAWSStore(StoreConfig{
		dynamodb: NewMockDynamoDB(0, 0),
		s3:       NewMockS3(),
	})
	if err != nil {
		t.Fatal(err)
	}
	store.CreateTables()
	defer store.Stop()

	ctx := user.WithID(context.Background(), "0")
	now := model.Now()
	chunks, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})

	chunk1 := NewChunk(
		model.Fingerprint(1),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "baz",
			"toms": "code",
		},
		&chunk.Desc{
			ChunkFirstTime: now.Add(-time.Hour),
			ChunkLastTime:  now,
			C:              chunks[0],
		},
	)
	chunk2 := NewChunk(
		model.Fingerprint(2),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "beep",
			"toms": "code",
		},
		&chunk.Desc{
			ChunkFirstTime: now.Add(-time.Hour),
			ChunkLastTime:  now,
			C:              chunks[0],
		},
	)

	if err := store.Put(ctx, []Chunk{chunk1, chunk2}); err != nil {
		t.Fatal(err)
	}

	test := func(name string, expect []Chunk, matchers ...*metric.LabelMatcher) {
		log.Infof(">>> %s", name)
		chunks, err := store.Get(ctx, now.Add(-time.Hour), now, matchers...)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expect, chunks) {
			t.Fatalf("%s: wrong chunks - %s", name, diff(expect, chunks))
		}
	}

	nameMatcher := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo")

	test("Just name label", []Chunk{chunk1, chunk2}, nameMatcher)
	test("Equal", []Chunk{chunk1}, nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "baz"))
	test("Not equal", []Chunk{chunk2}, nameMatcher, mustNewLabelMatcher(metric.NotEqual, "bar", "baz"))
	test("Regex match", []Chunk{chunk1, chunk2}, nameMatcher, mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz"))
	test("Multiple matchers", []Chunk{chunk1, chunk2}, nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz"))
	test("Multiple matchers II", []Chunk{chunk1}, nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.Equal, "bar", "baz"))
}

func mustNewLabelMatcher(matchType metric.MatchType, name model.LabelName, value model.LabelValue) *metric.LabelMatcher {
	matcher, err := metric.NewLabelMatcher(matchType, name, value)
	if err != nil {
		panic(err)
	}
	return matcher
}

func diff(want, have interface{}) string {
	text, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(spew.Sdump(want)),
		B:        difflib.SplitLines(spew.Sdump(have)),
		FromFile: "want",
		ToFile:   "have",
		Context:  3,
	})
	return "\n" + text
}
