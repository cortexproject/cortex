package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

type ParquetBucketStoreMetrics struct {
	regs *util.UserRegistries

	// TODO: Add some metrics
}

func NewParquetBucketStoreMetrics() *ParquetBucketStoreMetrics {
	m := &ParquetBucketStoreMetrics{
		regs: util.NewUserRegistries(),
	}

	return m
}

func (m *ParquetBucketStoreMetrics) AddUserRegistry(user string, reg *prometheus.Registry) {
	m.regs.AddUserRegistry(user, reg)
}

func (m *ParquetBucketStoreMetrics) RemoveUserRegistry(user string) {
	m.regs.RemoveUserRegistry(user, false)
}
