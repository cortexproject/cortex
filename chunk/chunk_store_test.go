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

func setupDynamodb(t *testing.T, dynamoDB DynamoDBClient) {
	tableManager, err := NewDynamoTableManager(TableManagerConfig{
		DynamoDB: dynamoDB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tableManager.syncTables(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChunkStoreUnprocessed(t *testing.T) {
	dynamoDB := NewMockDynamoDB(2, 2)
	setupDynamodb(t, dynamoDB)
	store, err := NewAWSStore(StoreConfig{
		DynamoDB: dynamoDB,
		S3:       NewMockS3(),
	})
	if err != nil {
		t.Fatal(err)
	}
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
	dynamoDB := NewMockDynamoDB(0, 0)
	setupDynamodb(t, dynamoDB)
	store, err := NewAWSStore(StoreConfig{
		DynamoDB: dynamoDB,
		S3:       NewMockS3(),
	})
	if err != nil {
		t.Fatal(err)
	}
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

func TestBigBuckets(t *testing.T) {
	scenarios := []struct {
		from, through, dailyBucketsFrom model.Time
		buckets                         []string
	}{
		// Buckets are by hour until we reach the `dailyBucketsFrom`, after which they are by day.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0).Add(1 * 24 * time.Hour),
			buckets:          []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "d1", "d2"},
		},

		// Only the day part of `dailyBucketsFrom` matters, not the time part.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0).Add(2*24*time.Hour) - 1,
			buckets:          []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "d1", "d2"},
		},

		// Moving dailyBucketsFrom to the previous day compared to the above makes 24 1-hour buckets disappear.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0).Add(1*24*time.Hour) - 1,
			buckets:          []string{"0", "d0", "d1", "d2"},
		},

		// If `dailyBucketsFrom` is after the interval, everything will be bucketed by hour.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(2 * 24 * time.Hour),
			dailyBucketsFrom: model.TimeFromUnix(0).Add(99 * 24 * time.Hour),
			buckets:          []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48"},
		},

		// Should only return daily buckets when dailyBucketsFrom is before the interval.
		{
			from:             model.TimeFromUnix(0).Add(1 * 24 * time.Hour),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0),
			buckets:          []string{"d1", "d2"},
		},
	}
	for i, s := range scenarios {
		expected := []bucketSpec{}
		for _, b := range s.buckets {
			expected = append(expected, bucketSpec{bucket: b})
		}

		cs := &AWSStore{
			cfg: StoreConfig{
				DailyBucketsFrom: s.dailyBucketsFrom,
			},
		}
		buckets := cs.bigBuckets(s.from, s.through)
		if !reflect.DeepEqual(buckets, expected) {
			t.Fatalf("%d. unexpected buckets; want %v, got %v", i, s.buckets, buckets)
		}
	}
}
