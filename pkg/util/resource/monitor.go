package resource

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/procfs"
	"github.com/weaveworks/common/httpgrpc"
	_ "go.uber.org/automaxprocs"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type ExhaustedError struct{}

func (e *ExhaustedError) Error() string {
	return "resource exhausted"
}

var ErrResourceExhausted = httpgrpc.Errorf(http.StatusTooManyRequests, "resource exhausted")

const heapMetricName = "/memory/classes/heap/objects:bytes"
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
	cpu  float64
	heap uint64
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
		cpu:  stat.CPUTime(),
		heap: s.metricSamples[0].Value.Uint64(),
	}, nil
}

type IMonitor interface {
	GetCPUUtilization() float64
	GetHeapUtilization() float64
	CheckResourceUtilization() (string, float64, float64, error)
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

func NewMonitor(thresholds configs.Resources, registerer prometheus.Registerer) (*Monitor, error) {
	m := &Monitor{
		thresholds: thresholds,
		lock:       sync.RWMutex{},
	}

	m.containerLimit.CPU = float64(runtime.GOMAXPROCS(0))
	m.containerLimit.Heap = float64(debug.SetMemoryLimit(-1))

	var err error
	m.scanner, err = NewScanner()
	if err != nil {
		return nil, err
	}

	m.cpuRates = make([]float64, dataPointsToAvg)
	m.cpuIntervals = make([]float64, dataPointsToAvg)
	m.Service = services.NewBasicService(nil, m.running, nil)

	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_resource_utilization",
		ConstLabels: map[string]string{"resource": "cpu"},
	}, m.GetCPUUtilization)
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_resource_utilization",
		ConstLabels: map[string]string{"resource": "heap"},
	}, m.GetHeapUtilization)
	promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name:        "cortex_resource_threshold",
		ConstLabels: map[string]string{"resource": "cpu"},
	}).Set(thresholds.CPU)
	promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name:        "cortex_resource_threshold",
		ConstLabels: map[string]string{"resource": "heap"},
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

	m.totalCPU -= m.cpuRates[m.index]
	m.totalInterval -= m.cpuIntervals[m.index]

	m.cpuRates[m.index] = stats.cpu - m.lastCPU
	m.cpuIntervals[m.index] = now.Sub(m.lastUpdate).Seconds()

	m.totalCPU += m.cpuRates[m.index]
	m.totalInterval += m.cpuIntervals[m.index]

	m.lastCPU = stats.cpu
	m.lastUpdate = now
	m.index = (m.index + 1) % dataPointsToAvg

	if m.totalInterval > 0 && m.containerLimit.CPU > 0 {
		m.utilization.CPU = m.totalCPU / m.totalInterval / m.containerLimit.CPU
	}
}

func (m *Monitor) storeHeapUtilization(stats Stats) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.utilization.Heap = float64(stats.heap) / m.containerLimit.Heap
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
		return "cpu", m.thresholds.CPU, cpu, httpgrpc.Errorf(http.StatusTooManyRequests, err.Error())
	}

	if m.thresholds.Heap > 0 && heap > m.thresholds.Heap {
		err := ExhaustedError{}
		return "heap", m.thresholds.Heap, heap, httpgrpc.Errorf(http.StatusTooManyRequests, err.Error())
	}

	return "", 0, 0, nil
}
