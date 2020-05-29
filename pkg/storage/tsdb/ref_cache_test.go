package tsdb

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestRefCache_GetAndSetReferences(t *testing.T) {
	now := time.Now()
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}

	c := NewRefCache()
	_, ok := c.Ref(now, ls1)
	assert.Equal(t, false, ok)

	_, ok = c.Ref(now, ls2)
	assert.Equal(t, false, ok)

	c.SetRef(now, ls1, 1)
	ref, ok := c.Ref(now, ls1)
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(1), ref)

	_, ok = c.Ref(now, ls2)
	assert.Equal(t, false, ok)

	c.SetRef(now, ls2, 2)
	ref, ok = c.Ref(now, ls2)
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(2), ref)

	// Overwrite a value with a new one
	c.SetRef(now, ls2, 3)
	ref, ok = c.Ref(now, ls2)
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(3), ref)
}

func TestRefCache_ShouldCorrectlyHandleFingerprintCollisions(t *testing.T) {
	now := time.Now()
	// The two following series have the same FastFingerprint=e002a3a451262627
	ls1 := []labels.Label{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}
	ls2 := []labels.Label{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}

	c := NewRefCache()
	c.SetRef(now, ls1, 1)
	c.SetRef(now, ls2, 2)

	ref, ok := c.Ref(now, ls1)
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(1), ref)

	ref, ok = c.Ref(now, ls2)
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(2), ref)
}

func TestRefCache_Purge(t *testing.T) {
	series := [][]labels.Label{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}},
		{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}},
	}

	// Run the same test for increasing TTL values
	for ttl := 0; ttl <= len(series); ttl++ {
		now := time.Unix(100, 0)
		c := NewRefCache()

		// Set the series to the cache with decreasing timestamps
		for i := 0; i < len(series); i++ {
			c.SetRef(now.Add(time.Duration(-i)*time.Second), series[i], uint64(i))
		}

		c.Purge(now.Add(time.Duration(-ttl) * time.Second))

		// Check retained and purged entries
		for i := 0; i <= ttl && i < len(series); i++ {
			ref, ok := c.Ref(now, series[i])
			assert.Equal(t, true, ok)
			assert.Equal(t, uint64(i), ref)
		}

		for i := ttl + 1; i < len(series); i++ {
			_, ok := c.Ref(now, series[i])
			assert.Equal(t, false, ok)
		}
	}
}

var series1M = prepareSeries(1e6)

func BenchmarkRefCacheConcurrency_1m_50(b *testing.B) {
	benchmarkRefCacheConcurrency(b, series1M, 50)
}

func BenchmarkRefCacheConcurrency_1m_250(b *testing.B) {
	benchmarkRefCacheConcurrency(b, series1M, 100)
}

func BenchmarkRefCacheConcurrency_1m_1000(b *testing.B) {
	benchmarkRefCacheConcurrency(b, series1M, 500)
}

func prepareSeries(numSeries int) []labels.Labels {
	series := make([]labels.Labels, numSeries)

	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{
			{Name: "a", Value: strconv.Itoa(s)},
		}
	}

	return series
}

func benchmarkRefCacheConcurrency(b *testing.B, series []labels.Labels, goroutines int) {
	c := NewRefCache()

	fn := func(wg *sync.WaitGroup, start chan struct{}, step int) {
		defer wg.Done()
		<-start

		for i := 0; i < b.N; i++ {
			now := time.Now()

			for s := 0; s < len(series); s += step {
				_, ok := c.Ref(now, series[s])
				if !ok {
					c.SetRef(now, series[s], uint64(s))
				}
			}
		}
	}

	start := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go fn(wg, start, 1+(i%10))
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkRefCache_SetRef(b *testing.B) {
	const numSeries = 10000

	// Prepare series
	series := [numSeries]labels.Labels{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}
	}

	now := time.Now()
	c := NewRefCache()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for s := 0; s < numSeries; s++ {
			c.SetRef(now, series[0], uint64(i))
		}
	}
}

func BenchmarkRefCache_Ref(b *testing.B) {
	const numSeries = 10000

	now := time.Now()
	c := NewRefCache()

	// Prepare series
	series := [numSeries]labels.Labels{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}
		c.SetRef(now, series[s], uint64(s))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for s := 0; s < numSeries; s++ {
			c.Ref(now, series[s])
		}
	}
}

func BenchmarkRefCache_purge(b *testing.B) {
	const numSeries = 1000000      // 1M
	const numExpiresSeries = 10000 // 10K

	now := time.Now()
	c := NewRefCache()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		series := [numSeries]labels.Labels{}
		for s := 0; s < numSeries; s++ {
			series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}

			if s < numExpiresSeries {
				c.SetRef(now.Add(-time.Minute), series[s], uint64(s))
			} else {
				c.SetRef(now, series[s], uint64(s))
			}
		}

		b.StartTimer()

		// Purge everything
		c.Purge(now)
	}
}
