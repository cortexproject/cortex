package testutils

import (
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
)

type ReadRingMock struct {
	mock.Mock
	prometheus.Collector
}

func NewReadRingMock() *ReadRingMock {
	return &ReadRingMock{}
}

func (m *ReadRingMock) Get(key uint32, op ring.Operation, buf []ring.IngesterDesc) (ring.ReplicationSet, error) {
	args := m.Called(key, op, buf)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (m *ReadRingMock) GetAll() (ring.ReplicationSet, error) {
	args := m.Called()
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (m *ReadRingMock) ReplicationFactor() int {
	args := m.Called()
	return args.Int(0)
}

func (m *ReadRingMock) IngesterCount() int {
	args := m.Called()
	return args.Int(0)
}
