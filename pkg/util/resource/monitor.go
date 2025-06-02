package resource

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util/services"
)

const (
	CPU  Type = "cpu"
	Heap Type = "heap"
)

type Type string

type IMonitor interface {
	GetCPUUtilization() float64
	GetHeapUtilization() float64
}

type Monitor struct {
	services.Service

	scanners       map[Type]scanner
	containerLimit map[Type]float64
	utilization    map[Type]float64
	interval       time.Duration

	// Variables to calculate average CPU utilization
	index         int
	cpuRates      []float64
	cpuIntervals  []float64
	totalCPU      float64
	totalInterval float64
	lastCPU       float64
	lastUpdate    time.Time
	cpuDataPoints int

	lock sync.RWMutex
}

func NewMonitor(limits map[Type]float64, interval, cpuRateInterval time.Duration, registerer prometheus.Registerer) (*Monitor, error) {
	m := &Monitor{
		containerLimit: limits,
		scanners:       make(map[Type]scanner),
		utilization:    make(map[Type]float64),
		interval:       interval,

		lock: sync.RWMutex{},
	}

	m.interval = interval
	m.cpuDataPoints = int(cpuRateInterval.Nanoseconds() / interval.Nanoseconds())
	m.cpuRates = make([]float64, m.cpuDataPoints)
	m.cpuIntervals = make([]float64, m.cpuDataPoints)

	m.Service = services.NewBasicService(nil, m.running, nil)

	for resType, limit := range limits {
		var scannerFunc func() (scanner, error)
		var gaugeFunc func() float64

		switch resType {
		case CPU:
			scannerFunc = newCPUScanner
			gaugeFunc = m.GetCPUUtilization
		case Heap:
			scannerFunc = newHeapScanner
			gaugeFunc = m.GetHeapUtilization
		default:
			return nil, fmt.Errorf("no scanner available for resource type: [%s]", resType)
		}

		s, err := scannerFunc()
		if err != nil {
			return nil, err
		}
		m.scanners[resType] = s
		m.containerLimit[resType] = limit

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "cortex_resource_utilization",
			ConstLabels: map[string]string{"resource": string(resType)},
		}, gaugeFunc)
	}

	return m, nil
}

func (m *Monitor) running(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			for resType, scanner := range m.scanners {
				val, err := scanner.scan()
				if err != nil {
					return fmt.Errorf("error scanning resource %s", resType)
				}

				switch resType {
				case CPU:
					m.storeCPUUtilization(val)
				case Heap:
					m.storeHeapUtilization(val)
				}
			}
		}
	}
}

func (m *Monitor) storeCPUUtilization(cpuTime float64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	now := time.Now()

	if m.lastUpdate.IsZero() {
		m.lastCPU = cpuTime
		m.lastUpdate = now
		return
	}

	m.totalCPU -= m.cpuRates[m.index]
	m.totalInterval -= m.cpuIntervals[m.index]

	m.cpuRates[m.index] = cpuTime - m.lastCPU
	m.cpuIntervals[m.index] = now.Sub(m.lastUpdate).Seconds()

	m.totalCPU += m.cpuRates[m.index]
	m.totalInterval += m.cpuIntervals[m.index]

	m.lastCPU = cpuTime
	m.lastUpdate = now
	m.index = (m.index + 1) % m.cpuDataPoints

	if m.totalInterval > 0 && m.containerLimit[CPU] > 0 {
		m.utilization[CPU] = m.totalCPU / m.totalInterval / m.containerLimit[CPU]
	}
}

func (m *Monitor) GetCPUUtilization() float64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.utilization[CPU]
}

func (m *Monitor) storeHeapUtilization(val float64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.containerLimit[Heap] > 0 {
		m.utilization[Heap] = val / m.containerLimit[Heap]
	}
}

func (m *Monitor) GetHeapUtilization() float64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.utilization[Heap]
}
