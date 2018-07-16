package chunk

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeapCache(t *testing.T) {
	c := newHeapCache(10)

	// Check put / get works
	for i := 0; i < 10; i++ {
		c.put(strconv.Itoa(i), i)
	}
	require.Len(t, c.index, 10)
	require.Len(t, c.entries, 10)

	for i := 0; i < 10; i++ {
		value, ok := c.get(strconv.Itoa(i))
		require.True(t, ok)
		require.Equal(t, i, value.(int))
	}

	// Check evictions
	for i := 10; i < 15; i++ {
		c.put(strconv.Itoa(i), i)
	}
	require.Len(t, c.index, 10)
	require.Len(t, c.entries, 10)

	for i := 0; i < 5; i++ {
		_, ok := c.get(strconv.Itoa(i))
		require.False(t, ok)
	}
	for i := 5; i < 15; i++ {
		value, ok := c.get(strconv.Itoa(i))
		require.True(t, ok)
		require.Equal(t, i, value.(int))
	}

	// Check updates work
	for i := 5; i < 15; i++ {
		c.put(strconv.Itoa(i), i*2)
	}
	require.Len(t, c.index, 10)
	require.Len(t, c.entries, 10)

	for i := 5; i < 15; i++ {
		value, ok := c.get(strconv.Itoa(i))
		require.True(t, ok)
		require.Equal(t, i*2, value.(int))
	}
}
