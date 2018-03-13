package client

import (
	io "io"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/cortex/pkg/util"
)

// Factory defines the signature for an ingester client factory
type Factory func(addr string, cfg Config) (IngesterClient, error)

// IngesterClientCache holds a cache of ingester clients
type IngesterClientCache struct {
	sync.RWMutex
	clients map[string]IngesterClient

	ingesterClientFactory Factory
	ingesterClientConfig  Config
}

// NewIngesterClientCache creates a new cache
func NewIngesterClientCache(factory Factory, config Config) *IngesterClientCache {
	return &IngesterClientCache{
		clients:               map[string]IngesterClient{},
		ingesterClientFactory: factory,
		ingesterClientConfig:  config,
	}
}

// GetClientFor gets the client for the specified address. If it does not exist it will make a new client
// at that address
func (cache *IngesterClientCache) GetClientFor(addr string) (IngesterClient, error) {
	cache.RLock()
	client, ok := cache.clients[addr]
	cache.RUnlock()
	if ok {
		return client, nil
	}

	cache.Lock()
	defer cache.Unlock()
	client, ok = cache.clients[addr]
	if ok {
		return client, nil
	}

	client, err := cache.ingesterClientFactory(addr, cache.ingesterClientConfig)
	if err != nil {
		return nil, err
	}
	cache.clients[addr] = client
	return client, nil
}

// RemoveClientFor removes the client with the specified address
func (cache *IngesterClientCache) RemoveClientFor(addr string) {
	cache.Lock()
	defer cache.Unlock()
	client, ok := cache.clients[addr]
	if ok {
		delete(cache.clients, addr)
		// Close in the background since this operation may take awhile and we have a mutex
		go func(addr string, closer io.Closer) {
			if err := closer.Close(); err != nil {
				level.Error(util.Logger).Log("msg", "error closing connection to ingester", "ingester", addr, "err", err)
			}
		}(addr, client.(io.Closer))
	}
}

// RegisteredAddresses returns all the addresses that a client is cached for
func (cache *IngesterClientCache) RegisteredAddresses() []string {
	result := []string{}
	cache.RLock()
	defer cache.RUnlock()
	for addr := range cache.clients {
		result = append(result, addr)
	}
	return result
}

// Count returns how many clients are in the cache
func (cache *IngesterClientCache) Count() int {
	cache.RLock()
	defer cache.RUnlock()
	return len(cache.clients)
}
