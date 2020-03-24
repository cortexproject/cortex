package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFifoCacheEviction(t *testing.T) {
	const (
		cnt       = 10
		overwrite = 5
	)
	entryTemplate := &cacheEntry{
		key:   "00",
		value: 0,
	}
	c := NewFifoCache("test1", FifoCacheConfig{MaxCount: cnt, Validity: 1 * time.Minute})
	ctx := context.Background()

	// Check put / get works
	keys := []string{}
	values := []interface{}{}
	for i := 0; i < cnt; i++ {
		keys = append(keys, fmt.Sprintf("%02d", i))
		values = append(values, i)
	}
	c.Put(ctx, keys, values)
	require.Len(t, c.entries, cnt)

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(1))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(cnt*sizeOf(entryTemplate)))

	for i := 0; i < cnt; i++ {
		value, ok := c.Get(ctx, fmt.Sprintf("%02d", i))
		require.True(t, ok)
		require.Equal(t, i, value.(int))
	}

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(1))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(cnt*sizeOf(entryTemplate)))

	// Check evictions
	keys = []string{}
	values = []interface{}{}
	for i := cnt; i < cnt+overwrite; i++ {
		keys = append(keys, fmt.Sprintf("%02d", i))
		values = append(values, i)
	}
	c.Put(ctx, keys, values)
	require.Len(t, c.entries, cnt)

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(2))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(cnt+overwrite))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(overwrite))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(cnt*sizeOf(entryTemplate)))

	for i := 0; i < cnt-overwrite; i++ {
		_, ok := c.Get(ctx, fmt.Sprintf("%02d", i))
		require.False(t, ok)
	}
	for i := cnt; i < cnt+overwrite; i++ {
		value, ok := c.Get(ctx, fmt.Sprintf("%02d", i))
		require.True(t, ok)
		require.Equal(t, i, value.(int))
	}

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(2))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(cnt+overwrite))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(overwrite))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(cnt*2))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(cnt-overwrite))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(cnt*sizeOf(entryTemplate)))

	// Check updates work
	keys = []string{}
	values = []interface{}{}
	for i := cnt; i < cnt+overwrite; i++ {
		keys = append(keys, fmt.Sprintf("%02d", i))
		values = append(values, i*2)
	}
	c.Put(ctx, keys, values)
	require.Len(t, c.entries, cnt)

	for i := cnt; i < cnt+overwrite; i++ {
		value, ok := c.Get(ctx, fmt.Sprintf("%02d", i))
		require.True(t, ok)
		require.Equal(t, i*2, value.(int))
	}

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(3))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(cnt+overwrite))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(overwrite))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(cnt))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(cnt*2+overwrite))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(cnt-overwrite))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(cnt*sizeOf(entryTemplate)))
}

func TestFifoCacheExpiry(t *testing.T) {
	key1, key2, key3, key4 := "01", "02", "03", "04"
	data1, data2, data3 := []float64{1.0, 2.0, 3.0}, "testdata", []byte{1, 2, 3, 4, 5, 6, 7, 8}
	entries := []*cacheEntry{
		{key: key1, value: data1},
		{key: key2, value: data2},
		{key: key3, value: data3},
	}
	var memorySz int
	for _, entry := range entries {
		memorySz += sizeOf(entry)
	}

	c := NewFifoCache("test2", FifoCacheConfig{MaxSize: memorySz, Validity: 5 * time.Millisecond})
	ctx := context.Background()

	c.Put(ctx,
		[]string{key1, key2, key4, key3, key2, key1},
		[]interface{}{[]int32{1, 2, 3, 4}, "dummy", []int{5, 4, 3}, data3, data2, data1})

	value, ok := c.Get(ctx, key1)
	require.True(t, ok)
	require.Equal(t, data1, value.([]float64))

	_, ok = c.Get(ctx, key4)
	require.False(t, ok)

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(1))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(5))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(2))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(3))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(2))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(1))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(0))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(memorySz))

	// Expire the entry.
	time.Sleep(5 * time.Millisecond)
	_, ok = c.Get(ctx, key1)
	require.False(t, ok)

	assert.Equal(t, testutil.ToFloat64(c.entriesAdded), float64(1))
	assert.Equal(t, testutil.ToFloat64(c.entriesAddedNew), float64(5))
	assert.Equal(t, testutil.ToFloat64(c.entriesEvicted), float64(2))
	assert.Equal(t, testutil.ToFloat64(c.entriesCurrent), float64(3))
	assert.Equal(t, testutil.ToFloat64(c.totalGets), float64(3))
	assert.Equal(t, testutil.ToFloat64(c.totalMisses), float64(2))
	assert.Equal(t, testutil.ToFloat64(c.staleGets), float64(1))
	assert.Equal(t, testutil.ToFloat64(c.memoryBytes), float64(memorySz))
}
