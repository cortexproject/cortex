package fragment_table

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSchedulerCoordination checks whether the hashtable for fragment-querier mapping gives the expected value
// It also checks if it remains functional and accurate during a multi-thread/concurrent read & write situation
func TestSchedulerCoordination(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		table := NewFragmentTable()
		table.AddMapping(uint64(0), uint64(1), "localhost:8000")
		table.AddMapping(uint64(0), uint64(2), "localhost:8001")

		result, exist := table.GetAllChildAddresses(uint64(0), []uint64{1, 2})
		require.True(t, exist)
		require.Equal(t, []string{"localhost:8000", "localhost:8001"}, result)

		result, exist = table.GetAllChildAddresses(uint64(0), []uint64{1, 3})
		require.False(t, exist)
		require.Empty(t, result)

		result, exist = table.GetAllChildAddresses(uint64(0), []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"localhost:8000"}, result)

		table.ClearMappings(uint64(0))
		result, exist = table.GetAllChildAddresses(uint64(0), []uint64{1})
		require.False(t, exist)
		require.Empty(t, result)
	})

	t.Run("concurrent operations", func(t *testing.T) {
		table := NewFragmentTable()
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines * 3)

		// write
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					queryID := uint64(routine)
					fragmentID := uint64(j)
					addr := fmt.Sprintf("localhost:%d", j)
					table.AddMapping(queryID, fragmentID, addr)
				}
			}(i)
		}

		// read
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					queryID := uint64(routine)
					fragmentIDs := []uint64{uint64(j)}
					table.GetAllChildAddresses(queryID, fragmentIDs)
				}
			}(i)
		}

		// clear
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					queryID := uint64(routine)
					table.ClearMappings(queryID)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("edge cases", func(t *testing.T) {
		table := NewFragmentTable()

		// test empty fragment IDs
		result, exist := table.GetAllChildAddresses(0, []uint64{})
		require.True(t, exist)
		require.Empty(t, result)

		// test clearing non-existent query
		table.ClearMappings(999)
		require.NotPanics(t, func() {
			table.ClearMappings(999)
		})

		// test overwriting mapping
		table.AddMapping(1, 1, "addr1")
		table.AddMapping(1, 1, "addr2")
		result, exist = table.GetAllChildAddresses(1, []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"addr2"}, result)

		// test multiple queries
		table.AddMapping(1, 1, "addr1")
		table.AddMapping(2, 1, "addr2")
		result, exist = table.GetAllChildAddresses(1, []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"addr1"}, result)

		result, exist = table.GetAllChildAddresses(2, []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"addr2"}, result)
	})
}
