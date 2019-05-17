package distributor

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/weaveworks/common/httpgrpc"

	"github.com/go-kit/kit/log"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/weaveworks/common/mtime"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
)

// ProtoReplicaDescFactory makes new InstanceDescs
func ProtoReplicaDescFactory() proto.Message {
	return NewReplicaDesc()
}

// NewReplicaDesc returns an empty *distributor.ReplicaDesc.
func NewReplicaDesc() *ReplicaDesc {
	return &ReplicaDesc{}
}

const (
	longPollDuration = 10 * time.Second
)

// Track the replica we're accepting samples from
// for each HA cluster we know about.
type haTracker struct {
	logger log.Logger
	cfg    HATrackerConfig
	client ring.KVClient

	// Replicas we are accepting samples from.
	electedLock sync.RWMutex
	elected     map[string]ReplicaDesc
	done        chan struct{}
	cancel      context.CancelFunc
}

// HATrackerConfig contains the configuration require to
// create a HA Tracker.
type HATrackerConfig struct {
	// We should only update the timestamp if the difference
	// between the stored timestamp and the time we received a sample at
	// is more than this duration.
	UpdateTimeout time.Duration
	// We should only failover to accepting samples from a replica
	// other than the replica written in the KVStore if the difference
	// between the stored timestamp and the time we received a sample is
	// more than this duration
	FailoverTimeout time.Duration

	KVStore ring.KVConfig
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *HATrackerConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.UpdateTimeout,
		"ha-tracker.update-timeout",
		15*time.Second,
		"Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed since the current stored timestamp.")
	f.DurationVar(&cfg.FailoverTimeout,
		"ha-tracker.failover-timeout",
		30*time.Second,
		"If we don't receive any samples from the accepted replica for a cluster in this amount of time we will failover to the next replica we receive a sample from. This value must be greater than the update timeout")
	// We want the ability to use different Consul instances for the ring and for HA cluster tracking.
	cfg.KVStore.RegisterFlagsWithPrefix("ha-tracker.", f)
}

// NewClusterTracker returns a new HA cluster tracker using either Consul
// or in-memory KV store.
func newClusterTracker(cfg HATrackerConfig) (*haTracker, error) {
	codec := ring.ProtoCodec{Factory: ProtoReplicaDescFactory}

	client, err := ring.NewKVStore(cfg.KVStore, codec)
	if err != nil {
		return nil, err
	}

	if cfg.FailoverTimeout <= cfg.UpdateTimeout {
		return nil, fmt.Errorf("HA Tracker failover timeout must be greater than update timeout, %d is <= %d", cfg.FailoverTimeout, cfg.UpdateTimeout)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := haTracker{
		logger:  util.Logger,
		cfg:     cfg,
		done:    make(chan struct{}),
		elected: map[string]ReplicaDesc{},
		client:  client,
		cancel:  cancel,
	}
	go t.loop(ctx)
	return &t, nil
}

// Follows pattern used by ring for WatchKey.
func (c *haTracker) loop(ctx context.Context) {
	defer close(c.done)
	// The KVStore config we gave when creating c should have contained a prefix,
	// which would have given us a prefixed KVStore client. So, we can pass empty string here.
	c.client.WatchPrefix(ctx, "", func(key string, value interface{}) bool {
		replica := value.(*ReplicaDesc)
		c.electedLock.Lock()
		defer c.electedLock.Unlock()
		c.elected[key] = *replica
		return true
	})
}

// Stop ends calls the trackers cancel function, which will end the loop for WatchPrefix.
func (c *haTracker) stop() {
	c.cancel()
	<-c.done
}

// CheckReplica checks the cluster and replica against the backing KVStore and local cache in the
// tracker c to see if we should accept the incomming sample. It will return an error if the sample
// should not be accepted. Note that internally this function does checks against the stored values
// and may modify the stored data, for example to failover between replicas after a certain period of time.
// A 202 response code is returned (from checkKVstore) if we shouldn't store this sample but are
// accepting samples from another replica for the cluster, so that there isn't a bunch of error's returned
// to customers clients.
func (c *haTracker) checkReplica(ctx context.Context, userID, cluster, replica string) error {
	key := fmt.Sprintf("%s/%s", userID, cluster)
	now := mtime.Now()
	c.electedLock.RLock()
	entry, ok := c.elected[key]
	c.electedLock.RUnlock()

	if ok && entry.Replica == replica && now.Sub(timestamp.Time(entry.ReceivedAt)) < c.cfg.UpdateTimeout {
		return nil
	}
	return c.checkKVStore(ctx, key, replica, now)
}

func (c *haTracker) checkKVStore(ctx context.Context, key, replica string, now time.Time) error {
	return c.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
		if desc, ok := in.(*ReplicaDesc); ok {

			// We don't need to CAS and update the timestamp in the KV store if the timestamp we've received
			// this sample at is less than updateTimeout amount of time since the timestamp in the KV store.
			if desc.Replica == replica && now.Sub(timestamp.Time(desc.ReceivedAt)) < c.cfg.UpdateTimeout {
				return nil, false, nil
			}

			// We shouldn't failover to accepting a new replica if the timestamp we've received this sample at
			// is less than failOver timeout amount of time since the timestamp in the KV store.
			if desc.Replica != replica && now.Sub(timestamp.Time(desc.ReceivedAt)) < c.cfg.FailoverTimeout {
				// Return a 202.
				return nil, false, httpgrpc.Errorf(http.StatusAccepted, "replicas did not match, rejecting sample: %s != %s", replica, desc.Replica)
			}
		}

		// There was either invalid or no data for the key, so we now accept samples
		// from this replica. Invalid could mean that the timestamp in the KV store was
		// out of date based on the update and failover timeouts when compared to now.
		return &ReplicaDesc{
			Replica: replica, ReceivedAt: timestamp.FromTime(now),
		}, true, nil
	})
}

// Modifies the labels parameter in place, removing labels that match
// the replica or cluster label and returning their values. Returns an error
// if we find one but not both of the labels.
func findHALabels(replicaLabel, clusterLabel string, labels []client.LabelAdapter) (string, string) {
	var cluster, replica string
	var pair client.LabelAdapter

	for _, pair = range labels {
		if pair.Name == replicaLabel {
			replica = string(pair.Value)
		}
		if pair.Name == clusterLabel {
			cluster = string(pair.Value)
		}
	}

	return cluster, replica
}
