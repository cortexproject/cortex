package queryeviction

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/configs"
	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/util/resource"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type mockMonitor struct {
	cpuUtil  atomic.Float64
	heapUtil atomic.Float64
}

func newMockMonitor(cpu, heap float64) *mockMonitor {
	m := &mockMonitor{}
	m.cpuUtil.Store(cpu)
	m.heapUtil.Store(heap)
	return m
}

func (m *mockMonitor) GetCPUUtilization() float64  { return m.cpuUtil.Load() }
func (m *mockMonitor) GetHeapUtilization() float64 { return m.heapUtil.Load() }

func testEvictorConfig(cpu, heap float64, cooldown int) configs.EvictionConfig {
	return configs.EvictionConfig{
		Threshold: configs.Threshold{
			CPUUtilization:  cpu,
			HeapUtilization: heap,
		},
		CheckInterval:        10 * time.Millisecond,
		CooldownPeriod:       cooldown,
		EvictionMetric:       "fetched_samples",
		MaxEvictionsPerCycle: 1,
	}
}

// startEvictor creates and starts an evictor, returning it and a cleanup function.
func startEvictor(t *testing.T, mon *mockMonitor, reg *QueryRegistry, cfg configs.EvictionConfig) *QueryEvictor {
	t.Helper()
	evictor := NewQueryEvictor(mon, reg, cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), "test")
	require.NotNil(t, evictor)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), evictor))
	t.Cleanup(func() { services.StopAndAwaitTerminated(context.Background(), evictor) }) //nolint:errcheck
	return evictor
}

// registerTestQuery registers a query and returns a channel closed on eviction.
func registerTestQuery(reg *QueryRegistry, fetchedSamples uint64, expr, user string) (uint64, chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	stats := &querier_stats.QueryStats{}
	stats.AddFetchedSamples(fetchedSamples)

	evicted := make(chan struct{})
	id := reg.Register(func() { cancel(); close(evicted) }, stats, expr, user, "")
	_ = ctx
	return id, evicted
}

func waitEvicted(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected query to be evicted")
	}
}

func assertNotEvicted(t *testing.T, ch chan struct{}, wait time.Duration) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("query should not have been evicted")
	case <-time.After(wait):
	}
}

func TestEviction_OccursWhenAboveThreshold(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evicted := registerTestQuery(reg, 1000, "up", "user1")
	startEvictor(t, newMockMonitor(0.95, 0.0), reg, testEvictorConfig(0.9, 0, 0))
	waitEvicted(t, evicted)
}

func TestNoEviction_WhenBelowThreshold(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evicted := registerTestQuery(reg, 1000, "up", "user1")
	startEvictor(t, newMockMonitor(0.5, 0.5), reg, testEvictorConfig(0.9, 0.9, 0))
	assertNotEvicted(t, evicted, 100*time.Millisecond)
	assert.Equal(t, 1, reg.Len())
}

func TestCooldown_BlocksEvictionThenResumes(t *testing.T) {
	mon := newMockMonitor(0.95, 0.0)
	reg := NewQueryRegistry(testMetricFunc)
	_, evicted1 := registerTestQuery(reg, 1000, "q1", "user1")

	// cooldown=3 ticks × 10ms = 30ms
	startEvictor(t, mon, reg, testEvictorConfig(0.9, 0, 3))
	waitEvicted(t, evicted1)

	// Second query registered during cooldown should not be evicted immediately.
	_, evicted2 := registerTestQuery(reg, 2000, "q2", "user2")
	assertNotEvicted(t, evicted2, 20*time.Millisecond)

	// But should be evicted after cooldown expires.
	waitEvicted(t, evicted2)
}

func TestCPUOnlyThreshold(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evicted := registerTestQuery(reg, 1000, "up", "user1")
	startEvictor(t, newMockMonitor(0.95, 0.95), reg, testEvictorConfig(0.9, 0, 0))
	waitEvicted(t, evicted)
}

func TestHeapOnlyThreshold(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evicted := registerTestQuery(reg, 1000, "up", "user1")
	startEvictor(t, newMockMonitor(0.0, 0.95), reg, testEvictorConfig(0, 0.9, 0))
	waitEvicted(t, evicted)
}

func TestCPUCheckedBeforeHeap(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evicted := registerTestQuery(reg, 1000, "up", "user1")
	evictor := startEvictor(t, newMockMonitor(0.95, 0.95), reg, testEvictorConfig(0.9, 0.9, 0))
	waitEvicted(t, evicted)

	assert.Equal(t, float64(1), promtest.ToFloat64(evictor.evictionsTotal.WithLabelValues(string(resource.CPU))))
	assert.Equal(t, float64(0), promtest.ToFloat64(evictor.evictionsTotal.WithLabelValues(string(resource.Heap))))
}

func TestEmptyRegistry_NoPanic(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	evictor := startEvictor(t, newMockMonitor(0.95, 0.95), reg, testEvictorConfig(0.9, 0.9, 0))

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, float64(0), promtest.ToFloat64(evictor.evictionsTotal.WithLabelValues(string(resource.CPU))))
	assert.Equal(t, float64(0), promtest.ToFloat64(evictor.evictionsTotal.WithLabelValues(string(resource.Heap))))
}

func TestCheckThresholds(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)

	tests := map[string]struct {
		cpuUtil, heapUtil       float64
		cpuThresh, heapThresh   float64
		wantResource            resource.Type
		wantUtil, wantThreshold float64
	}{
		"CPU breached":                {0.95, 0.5, 0.9, 0.9, resource.CPU, 0.95, 0.9},
		"Heap breached":               {0.5, 0.95, 0.9, 0.9, resource.Heap, 0.95, 0.9},
		"Both breached, CPU first":    {0.92, 0.93, 0.9, 0.9, resource.CPU, 0.92, 0.9},
		"Neither breached":            {0.5, 0.5, 0.9, 0.9, "", 0, 0},
		"CPU disabled, heap breached": {0.95, 0.95, 0, 0.9, resource.Heap, 0.95, 0.9},
		"Heap disabled, CPU breached": {0.95, 0.95, 0.9, 0, resource.CPU, 0.95, 0.9},
		"Exact threshold triggers":    {0.9, 0.5, 0.9, 0.9, resource.CPU, 0.9, 0.9},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mon := newMockMonitor(tc.cpuUtil, tc.heapUtil)
			cfg := configs.EvictionConfig{
				Threshold: configs.Threshold{
					CPUUtilization:  tc.cpuThresh,
					HeapUtilization: tc.heapThresh,
				},
				CheckInterval:        time.Second,
				EvictionMetric:       "fetched_samples",
				MaxEvictionsPerCycle: 1,
			}

			var evictor *QueryEvictor
			if cfg.Enabled() {
				evictor = NewQueryEvictor(mon, reg, cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), "test")
			} else {
				evictor = &QueryEvictor{monitor: mon, cfg: cfg}
			}

			resType, util, thresh := evictor.checkThresholds()
			assert.Equal(t, tc.wantResource, resType)
			assert.Equal(t, tc.wantUtil, util)
			assert.Equal(t, tc.wantThreshold, thresh)
		})
	}
}

func TestPrometheusMetrics_IncrementedCorrectly(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	evictor := startEvictor(t, newMockMonitor(0.95, 0.0), reg, testEvictorConfig(0.9, 0, 0))

	for i := range 3 {
		_, evicted := registerTestQuery(reg, uint64(1000+i), "q", "user")
		waitEvicted(t, evicted)
	}

	assert.Equal(t, float64(3), promtest.ToFloat64(evictor.evictionsTotal.WithLabelValues(string(resource.CPU))))
}

func TestNewQueryEvictor_ReturnsNilWhenDisabled(t *testing.T) {
	cfg := configs.EvictionConfig{CheckInterval: time.Second, EvictionMetric: "fetched_samples", MaxEvictionsPerCycle: 1}
	evictor := NewQueryEvictor(newMockMonitor(0, 0), NewQueryRegistry(testMetricFunc), cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), "test")
	assert.Nil(t, evictor)
}

func TestEviction_HeaviestQueryIsEvicted(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evictedSmall := registerTestQuery(reg, 100, "small", "user1")
	_, evictedLarge := registerTestQuery(reg, 10000, "large", "user2")
	_, evictedMedium := registerTestQuery(reg, 500, "medium", "user3")

	startEvictor(t, newMockMonitor(0.95, 0.0), reg, testEvictorConfig(0.9, 0, 0))

	select {
	case <-evictedLarge:
	case <-evictedSmall:
		t.Fatal("small query evicted before heaviest")
	case <-evictedMedium:
		t.Fatal("medium query evicted before heaviest")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected heaviest query to be evicted")
	}
}

func TestEviction_MultipleQueriesPerCycle(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, evictedSmall := registerTestQuery(reg, 100, "small", "user1")
	_, evictedLarge := registerTestQuery(reg, 10000, "large", "user2")
	_, evictedMedium := registerTestQuery(reg, 500, "medium", "user3")

	cfg := configs.EvictionConfig{
		Threshold: configs.Threshold{
			CPUUtilization: 0.9,
		},
		CheckInterval:        10 * time.Millisecond,
		CooldownPeriod:       300,
		EvictionMetric:       "fetched_samples",
		MaxEvictionsPerCycle: 2,
	}

	startEvictor(t, newMockMonitor(0.95, 0.0), reg, cfg)

	// The two heaviest (large=10000, medium=500) should be evicted in the same cycle.
	waitEvicted(t, evictedLarge)
	waitEvicted(t, evictedMedium)

	// The smallest should not be evicted because max per cycle is 2 and cooldown prevents another cycle.
	assertNotEvicted(t, evictedSmall, 50*time.Millisecond)
}
