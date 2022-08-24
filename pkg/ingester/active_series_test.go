package ingester

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func copyFn(l labels.Labels) labels.Labels { return l }

func TestActiveSeries_UpdateSeries(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}

	c := NewActiveSeries()
	assert.Equal(t, 0, c.Active())

	c.UpdateSeries(ls1, time.Now(), copyFn)
	assert.Equal(t, 1, c.Active())

	c.UpdateSeries(ls1, time.Now(), copyFn)
	assert.Equal(t, 1, c.Active())

	c.UpdateSeries(ls2, time.Now(), copyFn)
	assert.Equal(t, 2, c.Active())
}

func TestActiveSeries_Purge(t *testing.T) {
	series := [][]labels.Label{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		// The two following series have the same Fingerprint
		{{Name: "_", Value: "ypfajYg2lsv"}, {Name: "__name__", Value: "logs"}},
		{{Name: "_", Value: "KiqbryhzUpn"}, {Name: "__name__", Value: "logs"}},
	}

	// Run the same test for increasing TTL values
	for ttl := 0; ttl < len(series); ttl++ {
		c := NewActiveSeries()

		for i := 0; i < len(series); i++ {
			c.UpdateSeries(series[i], time.Unix(int64(i), 0), copyFn)
		}

		c.Purge(time.Unix(int64(ttl+1), 0))
		// call purge twice, just to hit "quick" path. It doesn't really do anything.
		c.Purge(time.Unix(int64(ttl+1), 0))

		exp := len(series) - (ttl + 1)
		assert.Equal(t, exp, c.Active())
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	c := NewActiveSeries()

	now := time.Now()
	c.UpdateSeries(ls1, now.Add(-2*time.Minute), copyFn)
	c.UpdateSeries(ls2, now, copyFn)
	c.Purge(now)

	assert.Equal(t, 1, c.Active())

	c.UpdateSeries(ls1, now.Add(-1*time.Minute), copyFn)
	c.UpdateSeries(ls2, now, copyFn)
	c.Purge(now)

	assert.Equal(t, 1, c.Active())

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, now.Add(-1*time.Minute), copyFn)
	c.Purge(now)

	assert.Equal(t, 1, c.Active())
}

var activeSeriesTestGoroutines = []int{50, 100, 500}

func BenchmarkActiveSeriesTest_single_series(b *testing.B) {
	for _, num := range activeSeriesTestGoroutines {
		b.Run(fmt.Sprintf("%d", num), func(b *testing.B) {
			benchmarkActiveSeriesConcurrencySingleSeries(b, num)
		})
	}
}

func benchmarkActiveSeriesConcurrencySingleSeries(b *testing.B, goroutines int) {
	series := labels.Labels{
		{Name: "a", Value: "a"},
	}

	c := NewActiveSeries()

	wg := &sync.WaitGroup{}
	start := make(chan struct{})
	max := int(math.Ceil(float64(b.N) / float64(goroutines)))

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			now := time.Now()

			for ix := 0; ix < max; ix++ {
				now = now.Add(time.Duration(ix) * time.Millisecond)
				c.UpdateSeries(series, now, copyFn)
			}
		}()
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkActiveSeries_UpdateSeries(b *testing.B) {
	c := NewActiveSeries()

	// Prepare series
	nameBuf := bytes.Buffer{}
	for i := 0; i < 50; i++ {
		nameBuf.WriteString("abcdefghijklmnopqrstuvzyx")
	}
	name := nameBuf.String()

	series := make([]labels.Labels, b.N)
	for s := 0; s < b.N; s++ {
		series[s] = labels.Labels{{Name: name, Value: name + strconv.Itoa(s)}}
	}

	now := time.Now().UnixNano()

	b.ResetTimer()
	for ix := 0; ix < b.N; ix++ {
		c.UpdateSeries(series[ix], time.Unix(0, now+int64(ix)), copyFn)
	}
}

func BenchmarkActiveSeries_Purge_once(b *testing.B) {
	benchmarkPurge(b, false)
}

func BenchmarkActiveSeries_Purge_twice(b *testing.B) {
	benchmarkPurge(b, true)
}

func benchmarkPurge(b *testing.B, twice bool) {
	const numSeries = 10000
	const numExpiresSeries = numSeries / 25

	now := time.Now()
	c := NewActiveSeries()

	series := [numSeries]labels.Labels{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, now.Add(-time.Minute), copyFn)
			} else {
				c.UpdateSeries(s, now, copyFn)
			}
		}

		assert.Equal(b, numSeries, c.Active())
		b.StartTimer()

		// Purge everything
		c.Purge(now)
		assert.Equal(b, numSeries-numExpiresSeries, c.Active())

		if twice {
			c.Purge(now)
			assert.Equal(b, numSeries-numExpiresSeries, c.Active())
		}
	}
}
