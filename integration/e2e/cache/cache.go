package e2ecache

import (
	"github.com/cortexproject/cortex/integration/e2e"
)

const (
	MemcachedPort = 11211
)

func NewMemcached() *e2e.ConcreteService {
	return e2e.NewConcreteService(
		"memcached",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"memcached:1.6.1",
		nil,
		e2e.NewTCPReadinessProbe(MemcachedPort),
		MemcachedPort,
	)
}
