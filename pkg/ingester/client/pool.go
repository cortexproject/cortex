package client

import (
	io "io"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/cortex/pkg/util"
)

// Factory defines the signature for an ingester client factory
type Factory func(addr string, cfg Config) (IngesterClient, error)

// IngesterPool holds a cache of ingester clients
type IngesterPool struct {
	sync.RWMutex
	clients map[string]IngesterClient

	ingesterClientFactory Factory
	ingesterClientConfig  Config
}

// NewIngesterPool creates a new cache
func NewIngesterPool(factory Factory, config Config) *IngesterPool {
	return &IngesterPool{
		clients:               map[string]IngesterClient{},
		ingesterClientFactory: factory,
		ingesterClientConfig:  config,
	}
}

// GetClientFor gets the client for the specified address. If it does not exist it will make a new client
// at that address
func (pool *IngesterPool) GetClientFor(addr string) (IngesterClient, error) {
	pool.RLock()
	client, ok := pool.clients[addr]
	pool.RUnlock()
	if ok {
		return client, nil
	}

	pool.Lock()
	defer pool.Unlock()
	client, ok = pool.clients[addr]
	if ok {
		return client, nil
	}

	client, err := pool.ingesterClientFactory(addr, pool.ingesterClientConfig)
	if err != nil {
		return nil, err
	}
	pool.clients[addr] = client
	return client, nil
}

// RemoveClientFor removes the client with the specified address
func (pool *IngesterPool) RemoveClientFor(addr string) {
	pool.Lock()
	defer pool.Unlock()
	client, ok := pool.clients[addr]
	if ok {
		delete(pool.clients, addr)
		// Close in the background since this operation may take awhile and we have a mutex
		go func(addr string, closer io.Closer) {
			if err := closer.Close(); err != nil {
				level.Error(util.Logger).Log("msg", "error closing connection to ingester", "ingester", addr, "err", err)
			}
		}(addr, client.(io.Closer))
	}
}

// RegisteredAddresses returns all the addresses that a client is cached for
func (pool *IngesterPool) RegisteredAddresses() []string {
	result := []string{}
	pool.RLock()
	defer pool.RUnlock()
	for addr := range pool.clients {
		result = append(result, addr)
	}
	return result
}

// Count returns how many clients are in the cache
func (pool *IngesterPool) Count() int {
	pool.RLock()
	defer pool.RUnlock()
	return len(pool.clients)
}
