package search

import (
	"context"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus-community/parquet-common/storage"
)

// RowRangesForConstraintsCache can be implemented to cache filter results for Constraint types
// The cache is keyed to the shard + row group, as row range indexes are zeroed to the row group.

type RowRangesForConstraintsCache interface {
	Get(ctx context.Context, shard storage.ParquetShard, rgIdx int, cs []Constraint) ([]RowRange, bool)
	Set(ctx context.Context, shard storage.ParquetShard, rgIdx int, cs []Constraint, rr []RowRange) error
	Delete(ctx context.Context, shard storage.ParquetShard, rgIdx int, cs []Constraint) error
	io.Closer
}

func constraintsCacheKey(shard storage.ParquetShard, rgIdx int, cs []Constraint) string {
	// <shard name>:rgidx-<row group index>:<constraint1>:<constraint2>:...:<constraintn>
	s := make([]string, len(cs)+2)
	s[0] = shard.Name()
	s[1] = "rgidx-" + strconv.Itoa(rgIdx)
	for i, c := range cs {
		s[i+2] = c.String()
	}
	return strings.Join(s, ":")
}

// ConstraintRowRangeCacheSyncMap implements a basic RowRangesForConstraintsCache with sync.Map.
// The sync.Map is optimized to offer write-once-read-many usage with minimal lock contention,
// which matches constraint filtering on the Parquet shard labels files which are not expected to change.
type ConstraintRowRangeCacheSyncMap struct {
	m *sync.Map
}

func NewConstraintRowRangeCacheSyncMap() *ConstraintRowRangeCacheSyncMap {
	return &ConstraintRowRangeCacheSyncMap{
		m: &sync.Map{},
	}
}

func (c ConstraintRowRangeCacheSyncMap) Get(_ context.Context, shard storage.ParquetShard, rgIdx int, cs []Constraint) ([]RowRange, bool) {
	key := constraintsCacheKey(shard, rgIdx, cs)
	rr, ok := c.m.Load(key)
	if ok {
		return rr.([]RowRange), ok
	}
	return nil, ok
}

func (c ConstraintRowRangeCacheSyncMap) Set(_ context.Context, shard storage.ParquetShard, rgIdx int, cs []Constraint, rr []RowRange) error {
	key := constraintsCacheKey(shard, rgIdx, cs)
	c.m.Store(key, rr)
	return nil
}

func (c ConstraintRowRangeCacheSyncMap) Delete(_ context.Context, shard storage.ParquetShard, rgIdx int, cs []Constraint) error {
	key := constraintsCacheKey(shard, rgIdx, cs)
	c.m.Delete(key)
	return nil
}

func (c ConstraintRowRangeCacheSyncMap) Close() error {
	c.m.Clear()
	return nil
}
