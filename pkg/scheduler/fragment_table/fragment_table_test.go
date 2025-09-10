package fragment_table

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFragmentTable(t *testing.T) {
	tests := []struct {
		name       string
		expiration time.Duration
	}{
		{
			name:       "with positive expiration",
			expiration: time.Hour,
		},
		{
			name:       "with small expiration",
			expiration: time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := NewFragmentTable(tt.expiration)
			require.NotNil(t, ft)
			require.NotNil(t, ft.mappings)
			assert.Equal(t, tt.expiration, ft.expiration)
		})
	}
}

func TestFragmentTable_AddAndGetAddress(t *testing.T) {
	ft := NewFragmentTable(time.Hour)

	tests := []struct {
		name      string
		queryID   uint64
		fragID    uint64
		addr      string
		wantFound bool
	}{
		{
			name:      "add new address",
			queryID:   1,
			fragID:    1,
			addr:      "addr1",
			wantFound: true,
		},
		{
			name:      "get non-existent address",
			queryID:   1,
			fragID:    2,
			wantFound: false,
		},
		{
			name:      "overwrite existing address",
			queryID:   1,
			fragID:    1,
			addr:      "addr2",
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.addr != "" {
				ft.AddAddressByID(tt.queryID, tt.fragID, tt.addr)
			}

			gotAddr, gotFound := ft.GetAddrByID(tt.queryID, tt.fragID)
			assert.Equal(t, tt.wantFound, gotFound)
			if tt.wantFound {
				assert.Equal(t, tt.addr, gotAddr)
			}
		})
	}
}

func TestFragmentTable_Expiration(t *testing.T) {
	expiration := 100 * time.Millisecond
	ft := NewFragmentTable(expiration)

	t.Run("entries expire after timeout", func(t *testing.T) {
		ft.AddAddressByID(1, 1, "addr1")

		// verify it exists
		addr, exists := ft.GetAddrByID(1, 1)
		require.True(t, exists)
		assert.Equal(t, "addr1", addr)

		// wait for expiration
		time.Sleep(expiration * 2)

		ft.cleanupExpired()

		// verify it's gone
		_, exists = ft.GetAddrByID(1, 1)
		assert.False(t, exists)
	})
}

func TestFragmentTable_ConcurrentAccess(t *testing.T) {
	ft := NewFragmentTable(time.Hour)

	const (
		numGoroutines = 10
		numOperations = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// start writers
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOperations {
				ft.AddAddressByID(uint64(id), uint64(j), "addr")
			}
		}(i)
	}

	// start readers
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for range numOperations {
			}
		}(i)
	}

	wg.Wait() // wait for all goroutines to finish
}

func TestFragmentTable_PeriodicCleanup(t *testing.T) {
	expiration := 100 * time.Millisecond
	ft := NewFragmentTable(expiration)

	ft.AddAddressByID(1, 1, "addr1")
	ft.AddAddressByID(1, 2, "addr2")

	// verify entries exist
	addr, ok := ft.GetAddrByID(1, 1)
	require.True(t, ok)
	require.Equal(t, "addr1", addr)

	addr, ok = ft.GetAddrByID(1, 2)
	require.True(t, ok)
	require.Equal(t, "addr2", addr)

	// wait for automatic cleanup
	time.Sleep(expiration * 3)

	// verify entries were cleaned up
	_, ok = ft.GetAddrByID(1, 1)
	require.False(t, ok)

	_, ok = ft.GetAddrByID(1, 2)
	require.False(t, ok)
}
