package ring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
)

type RingMock struct {
	mock.Mock
}

func (r *RingMock) GetInstanceIdByAddr(addr string) (string, error) {
	return "", nil
}

func (r *RingMock) Collect(ch chan<- prometheus.Metric) {}

func (r *RingMock) Describe(ch chan<- *prometheus.Desc) {}

func (r *RingMock) Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts []string, bufZones map[string]int) (ReplicationSet, error) {
	args := r.Called(key, op, bufDescs, bufHosts, bufZones)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetAllHealthy(op Operation) (ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetInstanceDescsForOperation(op Operation) (map[string]InstanceDesc, error) {
	args := r.Called(op)
	return args.Get(0).(map[string]InstanceDesc), args.Error(1)
}

func (r *RingMock) GetAllInstanceDescs(op Operation) ([]InstanceDesc, []InstanceDesc, error) {
	args := r.Called(op)
	return args.Get(0).([]InstanceDesc), make([]InstanceDesc, 0), args.Error(1)
}

func (r *RingMock) GetReplicationSetForOperation(op Operation) (ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) ReplicationFactor() int {
	return 0
}

func (r *RingMock) InstancesCount() int {
	return 0
}

func (r *RingMock) ShuffleShard(identifier string, size int) ReadRing {
	args := r.Called(identifier, size)
	return args.Get(0).(ReadRing)
}

func (r *RingMock) ShuffleShardWithZoneStability(identifier string, size int) ReadRing {
	args := r.Called(identifier, size)
	return args.Get(0).(ReadRing)
}

func (r *RingMock) GetInstanceState(instanceID string) (InstanceState, error) {
	args := r.Called(instanceID)
	return args.Get(0).(InstanceState), args.Error(1)
}

func (r *RingMock) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing {
	args := r.Called(identifier, size, lookbackPeriod, now)
	return args.Get(0).(ReadRing)
}

func (r *RingMock) HasInstance(instanceID string) bool {
	return true
}

func (r *RingMock) CleanupShuffleShardCache(identifier string) {}
