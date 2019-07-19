package usertracker

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/golang/protobuf/proto"
)

// ProtoUserUpdateDescFactory makes new UserUpdateDesc
func ProtoUserUpdateDescFactory() proto.Message {
	return NewUserUpdateDesc()
}

// NewUserUpdateDesc returns an empty *distributor.UserUpdateDesc.
func NewUserUpdateDesc() *UserUpdateDesc {
	return &UserUpdateDesc{}
}

// Tracker tracks when users update their configs to allow for efficient polling
type Tracker struct {
	logger log.Logger
	cfg    Config
	client kv.Client

	// Replicas we are accepting samples from.
	updatedUsersMtx sync.RWMutex
	updatedUsers    map[string]UserUpdateDesc
	done            chan struct{}
	cancel          context.CancelFunc
}

// Config contains the configuration require to create a User Tracker.
type Config struct {
	KVStore kv.Config
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	// We want the ability to use different Consul instances for the ring and for HA cluster tracking.
	cfg.KVStore.RegisterFlagsWithPrefix(prefix+"user-tracker.", f)
}

// NewTracker returns a new HA cluster tracker using either Consul
// or in-memory KV store.
func NewTracker(cfg Config) (*Tracker, error) {
	codec := codec.Proto{Factory: ProtoUserUpdateDescFactory}

	client, err := kv.NewClient(cfg.KVStore, codec)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := Tracker{
		logger:       util.Logger,
		cfg:          cfg,
		done:         make(chan struct{}),
		updatedUsers: map[string]UserUpdateDesc{},
		client:       client,
		cancel:       cancel,
	}
	go t.loop(ctx)
	return &t, nil
}

// Follows pattern used by ring for WatchKey.
func (c *Tracker) loop(ctx context.Context) {
	defer close(c.done)
	// The KVStore config we gave when creating c should have contained a prefix,
	// which would have given us a prefixed KVStore client. So, we can pass empty string here.
	c.client.WatchPrefix(ctx, "", func(key string, value interface{}) bool {
		replica := value.(*UserUpdateDesc)
		c.updatedUsersMtx.Lock()
		defer c.updatedUsersMtx.Unlock()
		c.updatedUsers[key] = *replica
		return true
	})
}

// Stop ends calls the trackers cancel function, which will end the loop for WatchPrefix.
func (c *Tracker) Stop() {
	c.cancel()
	<-c.done
}

// GetUpdatedUsers returns all of the users updated since the last
// poll
func (c *Tracker) GetUpdatedUsers(ctx context.Context) []string {
	c.updatedUsersMtx.Lock()
	defer c.updatedUsersMtx.Unlock()

	users := make([]string, len(c.updatedUsers))
	for u := range c.updatedUsers {
		users = append(users, u)
	}

	c.updatedUsers = map[string]UserUpdateDesc{}

	return users
}

// UpdateUser pushes a change to the kvstore to signal the user has been
// updated
func (c *Tracker) UpdateUser(ctx context.Context, userID string) error {
	return c.client.CAS(ctx, userID, func(in interface{}) (out interface{}, retry bool, err error) {
		// Add an entry to mark an update to a users rule configs
		return &UserUpdateDesc{
			User:      userID,
			UpdatedAt: time.Now().UnixNano(),
		}, true, nil
	})
}
