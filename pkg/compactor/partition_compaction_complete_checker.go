package compactor

import (
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/compact"
)

type PartitionCompactionBlockDeletableChecker struct{}

func NewPartitionCompactionBlockDeletableChecker() *PartitionCompactionBlockDeletableChecker {
	return &PartitionCompactionBlockDeletableChecker{}
}

func (p *PartitionCompactionBlockDeletableChecker) CanDelete(_ *compact.Group, _ ulid.ULID) bool {
	return false
}
