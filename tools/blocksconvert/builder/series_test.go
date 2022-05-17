package builder

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

type testSeries struct {
	l                labels.Labels
	cs               []chunks.Meta
	samples          uint64
	minTime, maxTime int64
}

func TestSeries(t *testing.T) {
	series := map[string]testSeries{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	const seriesCount = 100
	for i := 0; i < seriesCount; i++ {
		l := labels.Labels{labels.Label{Name: generateString(r), Value: generateString(r)}}
		series[l.String()] = testSeries{
			l:       l,
			cs:      []chunks.Meta{{Ref: chunks.ChunkRef(r.Uint64()), MinTime: r.Int63(), MaxTime: r.Int63()}},
			samples: r.Uint64(),
			minTime: r.Int63(),
			maxTime: r.Int63(),
		}
	}

	sl := newSeriesList(seriesCount/7, t.TempDir())

	symbolsMap := map[string]bool{}

	for _, s := range series {
		require.NoError(t, sl.addSeries(s.l, s.cs, s.samples, s.minTime, s.maxTime))

		for _, l := range s.l {
			symbolsMap[l.Name] = true
			symbolsMap[l.Value] = true
		}
	}
	require.NoError(t, sl.flushSeries())

	symbols := make([]string, 0, len(symbolsMap))
	for s := range symbolsMap {
		symbols = append(symbols, s)
	}
	sort.Strings(symbols)

	sit, err := sl.symbolsIterator()
	require.NoError(t, err)

	for _, exp := range symbols {
		s, ok := sit.Next()
		require.True(t, ok)
		require.Equal(t, exp, s)
	}
	_, ok := sit.Next()
	require.False(t, ok)
	require.NoError(t, sit.Error())
	require.NoError(t, sit.Close())

	prevLabels := labels.Labels{}

	rit, err := sl.seriesIterator()
	require.NoError(t, err)

	for len(series) > 0 {
		s, ok := rit.Next()
		require.True(t, ok)

		es, ok := series[s.Metric.String()]
		require.True(t, ok)
		require.True(t, labels.Compare(prevLabels, s.Metric) < 0)

		prevLabels = s.Metric

		require.Equal(t, 0, labels.Compare(es.l, s.Metric))

		for ix, c := range es.cs {
			require.True(t, ix < len(s.Chunks))
			require.Equal(t, c.Ref, s.Chunks[ix].Ref)
			require.Equal(t, c.MinTime, s.Chunks[ix].MinTime)
			require.Equal(t, c.MaxTime, s.Chunks[ix].MaxTime)
		}

		require.Equal(t, es.minTime, s.MinTime)
		require.Equal(t, es.maxTime, s.MaxTime)
		require.Equal(t, es.samples, s.Samples)

		delete(series, s.Metric.String())
	}

	_, ok = rit.Next()
	require.False(t, ok)
	require.NoError(t, rit.Error())
	require.NoError(t, rit.Close())
}

func generateString(r *rand.Rand) string {
	buf := bytes.Buffer{}

	chars := "abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ01234567890_"

	for l := 20 + r.Intn(100); l > 0; l-- {
		buf.WriteByte(chars[r.Intn(len(chars))])
	}

	return buf.String()
}
