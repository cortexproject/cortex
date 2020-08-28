package index

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestIndex(t *testing.T) {
	index := New()

	for _, entry := range []struct {
		m  model.Metric
		fp model.Fingerprint
	}{
		{model.Metric{"foo": "bar", "flip": "flop"}, 3},
		{model.Metric{"foo": "bar", "flip": "flap"}, 2},
		{model.Metric{"foo": "baz", "flip": "flop"}, 1},
		{model.Metric{"foo": "baz", "flip": "flap"}, 0},
	} {
		index.Add(client.FromMetricsToLabelAdapters(entry.m), entry.fp)
	}

	for _, tc := range []struct {
		matchers []*labels.Matcher
		fps      []model.Fingerprint
	}{
		{nil, nil},
		{mustParseMatcher(`{fizz="buzz"}`), []model.Fingerprint{}},

		{mustParseMatcher(`{foo="bar"}`), []model.Fingerprint{2, 3}},
		{mustParseMatcher(`{foo="baz"}`), []model.Fingerprint{0, 1}},
		{mustParseMatcher(`{flip="flop"}`), []model.Fingerprint{1, 3}},
		{mustParseMatcher(`{flip="flap"}`), []model.Fingerprint{0, 2}},

		{mustParseMatcher(`{foo="bar", flip="flop"}`), []model.Fingerprint{3}},
		{mustParseMatcher(`{foo="bar", flip="flap"}`), []model.Fingerprint{2}},
		{mustParseMatcher(`{foo="baz", flip="flop"}`), []model.Fingerprint{1}},
		{mustParseMatcher(`{foo="baz", flip="flap"}`), []model.Fingerprint{0}},

		{mustParseMatcher(`{fizz=~"b.*"}`), []model.Fingerprint{}},

		{mustParseMatcher(`{foo=~"bar.*"}`), []model.Fingerprint{2, 3}},
		{mustParseMatcher(`{foo=~"ba.*"}`), []model.Fingerprint{0, 1, 2, 3}},
		{mustParseMatcher(`{flip=~"flop|flap"}`), []model.Fingerprint{0, 1, 2, 3}},
		{mustParseMatcher(`{flip=~"flaps"}`), []model.Fingerprint{}},

		{mustParseMatcher(`{foo=~"bar|bax", flip="flop"}`), []model.Fingerprint{3}},
		{mustParseMatcher(`{foo=~"bar|baz", flip="flap"}`), []model.Fingerprint{0, 2}},
		{mustParseMatcher(`{foo=~"baz.+", flip="flop"}`), []model.Fingerprint{}},
		{mustParseMatcher(`{foo=~"baz", flip="flap"}`), []model.Fingerprint{0}},
	} {
		assert.Equal(t, tc.fps, index.Lookup(tc.matchers))
	}

	assert.Equal(t, []string{"flip", "foo"}, index.LabelNames())
	assert.Equal(t, []string{"bar", "baz"}, index.LabelValues("foo"))
	assert.Equal(t, []string{"flap", "flop"}, index.LabelValues("flip"))
}

func BenchmarkSetRegexLookup(b *testing.B) {
	// Prepare the benchmark.
	seriesLabels := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	seriesPerLabel := 100000

	idx := New()
	for _, l := range seriesLabels {
		for i := 0; i < seriesPerLabel; i++ {
			lbls := labels.FromStrings("foo", l, "bar", strconv.Itoa(i))
			idx.Add(client.FromLabelsToLabelAdapters(lbls), model.Fingerprint(lbls.Hash()))
		}
	}

	selectionLabels := []string{}
	for i := 0; i < 100; i++ {
		selectionLabels = append(selectionLabels, strconv.Itoa(i))
	}

	tests := []struct {
		name    string
		matcher string
	}{
		{
			name:    "select all",
			matcher: fmt.Sprintf(`{bar=~"%s"}`, strings.Join(selectionLabels, "|")),
		},
		{
			name:    "select two",
			matcher: fmt.Sprintf(`{bar=~"%s"}`, strings.Join(selectionLabels[:2], "|")),
		},
		{
			name:    "select half",
			matcher: fmt.Sprintf(`{bar=~"%s"}`, strings.Join(selectionLabels[:len(selectionLabels)/2], "|")),
		},
		{
			name:    "select none",
			matcher: `{bar=~"bleep|bloop"}`,
		},
		{
			name:    "equality matcher",
			matcher: `{bar="1"}`,
		},
		{
			name:    "regex (non-set) matcher",
			matcher: `{bar=~"1.*"}`,
		},
	}

	b.ResetTimer()

	for _, tc := range tests {
		b.Run(fmt.Sprintf("%s:%s", tc.name, tc.matcher), func(b *testing.B) {
			matcher := mustParseMatcher(tc.matcher)
			for n := 0; n < b.N; n++ {
				idx.Lookup(matcher)
			}
		})
	}

}

func mustParseMatcher(s string) []*labels.Matcher {
	ms, err := parser.ParseMetricSelector(s)
	if err != nil {
		panic(err)
	}
	return ms
}

func BenchmarkIndexValueEntry_Add(b *testing.B) {
	var fingerprintsGen = func(size int) func(i int) model.Fingerprint {
		fps := make([]model.Fingerprint, size)
		for i := 0; i < size; i++ {
			fps[i] = model.Fingerprint(rand.Uint64())
		}
		return func(i int) model.Fingerprint {
			if i >= size {
				i = i % size
			}
			return fps[i]
		}
	}
	getFingerprints := fingerprintsGen(100000)

	c := newIndexValueEntry("")
	b.Run("shard", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.add(getFingerprints(i))
		}
	})

	b.Run("plain", func(b *testing.B) {
		var fps []model.Fingerprint
		for i := 0; i < b.N; i++ {
			fingerprint := getFingerprints(i)
			j := sort.Search(len(fps), func(i int) bool {
				return fps[i] >= fingerprint
			})
			fps = append(fps, 0)
			copy(fps[j+1:], fps[j:])
			fps[j] = fingerprint
		}
	})

}

func TestIndexValueEntry(t *testing.T) {
	const size = 100
	c := newIndexValueEntry("value")
	var fps []model.Fingerprint
	for i := 0; i < size; i++ {
		fingerprint := model.Fingerprint(rand.Uint64())

		// add element into fps
		j := sort.Search(len(fps), func(i int) bool {
			return fps[i] >= fingerprint
		})
		fps = append(fps, 0)
		copy(fps[j+1:], fps[j:])
		fps[j] = fingerprint

		c.add(fingerprint)

		assert.Equal(t, fps, c.fps())
		assert.Equal(t, len(c.fps()), c.length())
	}

	for len(fps) > 0 {
		fp := fps[rand.Intn(len(fps))]

		// delete element in fps
		j := sort.Search(len(fps), func(i int) bool {
			return fps[i] >= fp
		})
		fps = fps[:j+copy(fps[j:], fps[j+1:])]

		c.delete(fp)

		assert.Equal(t, fps, c.fps())
	}
}
