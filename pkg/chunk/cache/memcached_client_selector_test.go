package cache_test

import (
	"fmt"
	"net"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemcachedJumpHashSelector_PickSever(t *testing.T) {
	s := cache.MemcachedJumpHashSelector{}
	err := s.SetServers("google.com:80", "microsoft.com:80", "duckduckgo.com:80")
	require.NoError(t, err)

	distribution := make(map[net.Addr]int)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		addr, err := s.PickServer(key)
		if assert.NoError(t, err) {
			distribution[addr]++
		}
	}

	// All of the servers should have been returned at least
	// once
	for _, v := range distribution {
		assert.NotZero(t, v)
	}
}

func TestMemcachedJumpHashSelector_PickSever_ErrNoServers(t *testing.T) {
	s := cache.MemcachedJumpHashSelector{}
	_, err := s.PickServer("foo")
	assert.Error(t, memcache.ErrNoServers, err)
}
