package resource

import (
	"context"
	"fmt"
	"net/http"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/procfs"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type ExhaustedError struct{}

func (e *ExhaustedError) Error() string {
	return "resource exhausted"
}

const heapMetricName = "/memory/classes/Heap/objects:bytes"
const monitorInterval = time.Second
const dataPointsToAvg = 30

type IScanner interface {
	Scan() (Stats, error)
}

type Scanner struct {
	proc          procfs.Proc
	metricSamples []metrics.Sample
}

type Stats struct {
	CPU  float64
	Heap uint64
}

func NewScanner() (*Scanner, error) {
	proc, err := procfs.Self()
	if err != nil {
		return nil, errors.Wrap(err, "error reading proc directory")
	}

	metricSamples := make([]metrics.Sample, 1)
	metricSamples[0].Name = heapMetricName
	metrics.Read(metricSamples)

	for _, sample := range metricSamples {
		if sample.Value.Kind() == metrics.KindBad {
			return nil, fmt.Errorf("metric %s is not supported", sample.Name)
		}
	}

	return &Scanner{
		proc:          proc,
		metricSamples: metricSamples,
	}, nil
}

func (s *Scanner) Scan() (Stats, error) {
	stat, err := s.proc.Stat()
	if err != nil {
		return Stats{}, err
	}

	metrics.Read(s.metricSamples)

	return Stats{
		CPU:  stat.CPUTime(),
		Heap: s.metricSamples[0].Value.Uint64(),
	}, nil
}

type Monitor struct {
	services.Service

	scanner IScanner

	containerLimit configs.Resources
	utilization    configs.Resources
	thresholds     configs.Resources

	// Variables to calculate average CPU utilization
	index         int
	cpuRates      []float64
	cpuIntervals  []float64
	totalCPU      float64
	totalInterval float64
	lastCPU       float64
	lastUpdate    time.Time

	lock sync.RWMutex
}

func NewMonitor(thresholds configs.Resources, limits configs.Resources, scanner IScanner, registerer prometheus.Registerer) (*Monitor, error) {
	m := &Monitor{
		thresholds:     thresholds,
		containerLimit: limits,
		scanner:        scanner,

		cpuRates:     make([]float64, dataPointsToAvg),
		cpuIntervals: make([]float64, dataPointsToAvg),

		lock: sync.RWMutex{},
	}

	m.Service = services.NewBasicService(nil, m.running, nil)

	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_resource_utilization",
		ConstLabels: map[string]string{"resource": "CPU"},
	}, m.GetCPUUtilization)
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_resource_utilization",
		ConstLabels: map[string]string{"resource": "Heap"},
	}, m.GetHeapUtilization)
	promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name:        "cortex_resource_threshold",
		ConstLabels: map[string]string{"resource": "CPU"},
	}).Set(thresholds.CPU)
	promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name:        "cortex_resource_threshold",
		ConstLabels: map[string]string{"resource": "Heap"},
	}).Set(thresholds.Heap)

	return m, nil
}

func (m *Monitor) running(ctx context.Context) error {
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			stats, err := m.scanner.Scan()
			if err != nil {
				return errors.Wrap(err, "error scanning metrics")
			}

			m.storeCPUUtilization(stats)
			m.storeHeapUtilization(stats)
		}
	}
}

func (m *Monitor) storeCPUUtilization(stats Stats) {
	m.lock.Lock()
	defer m.lock.Unlock()

	now := time.Now()

	if m.lastUpdate.IsZero() {
		m.lastCPU = stats.CPU
		m.lastUpdate = now
		return
	}

	m.totalCPU -= m.cpuRates[m.index]
	m.totalInterval -= m.cpuIntervals[m.index]

	m.cpuRates[m.index] = stats.CPU - m.lastCPU
	m.cpuIntervals[m.index] = now.Sub(m.lastUpdate).Seconds()

	m.totalCPU += m.cpuRates[m.index]
	m.totalInterval += m.cpuIntervals[m.index]

	m.lastCPU = stats.CPU
	m.lastUpdate = now
	m.index = (m.index + 1) % dataPointsToAvg

	if m.totalInterval > 0 && m.containerLimit.CPU > 0 {
		m.utilization.CPU = m.totalCPU / m.totalInterval / m.containerLimit.CPU
	}
}

func (m *Monitor) storeHeapUtilization(stats Stats) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.containerLimit.Heap > 0 {
		m.utilization.Heap = float64(stats.Heap) / m.containerLimit.Heap
	}
}

func (m *Monitor) GetCPUUtilization() float64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.utilization.CPU
}

func (m *Monitor) GetHeapUtilization() float64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.utilization.Heap
}

func (m *Monitor) CheckResourceUtilization() (string, float64, float64, error) {
	cpu := m.GetCPUUtilization()
	heap := m.GetHeapUtilization()

	if m.thresholds.CPU > 0 && cpu > m.thresholds.CPU {
		err := ExhaustedError{}
		return "CPU", m.thresholds.CPU, cpu, httpgrpc.Errorf(http.StatusTooManyRequests, "%s", err.Error())
	}

	if m.thresholds.Heap > 0 && heap > m.thresholds.Heap {
		err := ExhaustedError{}
		return "Heap", m.thresholds.Heap, heap, httpgrpc.Errorf(http.StatusTooManyRequests, "%s", err.Error())
	}

	return "", 0, 0, nil
}
