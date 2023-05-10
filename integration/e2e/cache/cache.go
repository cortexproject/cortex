package e2ecache

import (
	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2e/images"
)

const (
	MemcachedPort = 11211
	RedisPort     = 6379
)

func NewMemcached() *e2e.ConcreteService {
	return e2e.NewConcreteService(
		"memcached",
		images.Memcached,
		nil,
		e2e.NewTCPReadinessProbe(MemcachedPort),
		MemcachedPort,
	)
}

func NewRedis() *e2e.ConcreteService {
	return e2e.NewConcreteService(
		"redis",
		images.Redis,
		e2e.NewCommand("redis-server", "*:6379"),
		e2e.NewTCPReadinessProbe(RedisPort),
		RedisPort,
	)
}
