package testutils

import (
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
)

// ReadRingMock mocks a ReadRing
type ReadRingMock struct {
	mock.Mock
	prometheus.Collector
}

// NewReadRingMock creates a new ReadRing mock
func NewReadRingMock() *ReadRingMock {
	return &ReadRingMock{}
}

// Get mocks ReadRing's Get
func (m *ReadRingMock) Get(key uint32, op ring.Operation, buf []ring.IngesterDesc) (ring.ReplicationSet, error) {
	args := m.Called(key, op, buf)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

// GetAll mocks ReadRing's GetAll
func (m *ReadRingMock) GetAll() (ring.ReplicationSet, error) {
	args := m.Called()
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

// ReplicationFactor mocks ReadRing's ReplicationFactor
func (m *ReadRingMock) ReplicationFactor() int {
	args := m.Called()
	return args.Int(0)
}

// IngesterCount mocks ReadRing's IngesterCount
func (m *ReadRingMock) IngesterCount() int {
	args := m.Called()
	return args.Int(0)
}
