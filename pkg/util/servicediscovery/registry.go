package servicediscovery

import (
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/golang/protobuf/proto"
)

// ReadRegistry represents the read-only interface to the service registry.
type ReadRegistry interface {
	HealthyCount() int
}

// ServiceRegistryDescFactory returns a factory to make new ServiceRegistryDesc
func ServiceRegistryDescFactory() proto.Message {
	return NewServiceRegistryDesc()
}

// GetServiceRegistryCodec returns the codec to use for the KVStore
func GetServiceRegistryCodec() codec.Codec {
	return codec.Proto{Factory: ServiceRegistryDescFactory}
}

// NewServiceRegistryDesc makes new ServiceRegistryDesc
func NewServiceRegistryDesc() *ServiceRegistryDesc {
	return &ServiceRegistryDesc{
		Instances: map[string]ServiceInstanceDesc{},
	}
}

func (d *ServiceRegistryDesc) ensureInstance(id string, now time.Time) (created bool) {
	if d.Instances == nil {
		d.Instances = map[string]ServiceInstanceDesc{}
	}

	instanceDesc, ok := d.Instances[id]

	// If the service instance is already registered, we've just to update the
	// heartbeat timestamp
	if ok {
		instanceDesc.Timestamp = now.UnixNano()
		d.Instances[id] = instanceDesc

		return false
	}

	// Register the service
	d.Instances[id] = ServiceInstanceDesc{
		Id:        id,
		Timestamp: now.UnixNano(),
	}

	return true
}

func (d *ServiceRegistryDesc) removeInstance(id string) {
	if d.Instances == nil {
		return
	}

	delete(d.Instances, id)
}

func (d *ServiceRegistryDesc) removeExpired(timeout time.Duration, now time.Time) {
	if d.Instances == nil {
		return
	}

	oldestHeartbeatAccepted := now.Add(-timeout).UnixNano()

	for id, service := range d.Instances {
		if service.Timestamp < oldestHeartbeatAccepted {
			delete(d.Instances, id)
		}
	}
}

func (d *ServiceRegistryDesc) count() int {
	if d.Instances == nil {
		return 0
	}

	return len(d.Instances)
}
