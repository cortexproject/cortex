package distributor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/mtime"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
)

var (
	ctxUser1 = user.InjectOrgID(context.Background(), "user1")
	ctxUser2 = user.InjectOrgID(context.Background(), "user1")
)

func checkReplicaTimestamp(ctx context.Context, c *haTracker, user, cluster, replica string, expected time.Time) error {
	var err error
	ticker := time.NewTicker(time.Millisecond * 50)
	key := fmt.Sprintf("%s/%s", user, cluster)

	// Round the expected timestamp with milliseconds precision
	// to match "received at" precision
	expected = expected.Truncate(time.Millisecond)

outer:
	for {
		select {
		case <-ticker.C:
			c.electedLock.RLock()
			r := c.elected[key]
			c.electedLock.RUnlock()

			// If the replica or the timestamp don't match, we save the error
			// to return if the expected match is not found within the timeout period
			if r.GetReplica() != replica {
				err = fmt.Errorf("replicas did not match: %s != %s", r.GetReplica(), replica)
				continue outer
			} else if !timestamp.Time(r.GetReceivedAt()).Equal(expected) {
				err = fmt.Errorf("timestamps did not match: %+v != %+v", timestamp.Time(r.GetReceivedAt()), expected)
				continue outer
			}

			return nil
		case <-ctx.Done():
			break outer
		}
	}

	return err
}

func TestFailoverGreaterUpdate(t *testing.T) {
	type testCase struct {
		in   HATrackerConfig
		fail bool
	}

	cases := []testCase{
		{
			in: HATrackerConfig{
				EnableHATracker:        true,
				UpdateTimeout:          time.Second,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        time.Second,
				KVStore: kv.Config{
					Store: "inmemory",
				},
			},
			fail: true,
		},
		{
			in: HATrackerConfig{
				EnableHATracker:        true,
				UpdateTimeout:          time.Second,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        1999 * time.Millisecond,
				KVStore: kv.Config{
					Store: "inmemory",
				},
			},
			fail: true,
		},
		{
			in: HATrackerConfig{
				EnableHATracker:        true,
				UpdateTimeout:          time.Second,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        2000 * time.Millisecond,
				KVStore: kv.Config{
					Store: "inmemory",
				},
			},
			fail: false,
		},
		{
			in: HATrackerConfig{
				EnableHATracker:        true,
				UpdateTimeout:          time.Second,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        2001 * time.Millisecond,
				KVStore: kv.Config{
					Store: "inmemory",
				},
			},
			fail: false,
		},
	}

	for _, c := range cases {
		err := c.in.Validate()
		fail := err != nil
		assert.Equal(t, c.fail, fail, "unexpected result: %s", err)
	}
}

// Test that values are set in the HATracker after WatchPrefix has found it in the KVStore.
func TestWatchPrefixAssignment(t *testing.T) {
	cluster := "c1"
	replica := "r1"
	start := mtime.Now()

	codec := codec.Proto{Factory: ProtoReplicaDescFactory}
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Millisecond * 2,
	})
	assert.NoError(t, err)

	// Write the first time.
	mtime.NowForce(start)
	err = c.checkReplica(context.Background(), "user", cluster, replica)
	assert.NoError(t, err)

	// We need to wait for WatchPrefix to grab the value.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	// Check to see if the value in the trackers cache is correct.
	err = checkReplicaTimestamp(ctx, c, "user", cluster, replica, start)
	cancel()
	assert.NoError(t, err)
}

func TestCheckReplicaOverwriteTimeout(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"
	start := mtime.Now()

	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Store: "inmemory"},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	})
	assert.NoError(t, err)

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "test", replica1)
	assert.NoError(t, err)

	// Throw away a sample from replica2.
	err = c.checkReplica(context.Background(), "user", "test", replica2)
	assert.Error(t, err)

	// Wait more than the overwrite timeout.
	mtime.NowForce(start.Add(1100 * time.Millisecond))

	// Accept from replica 2, this should overwrite the saved replica of replica 1.
	err = c.checkReplica(context.Background(), "user", "test", replica2)
	assert.NoError(t, err)

	// We timed out accepting samples from replica 1 and should now reject them.
	err = c.checkReplica(context.Background(), "user", "test", replica1)
	assert.Error(t, err)
}

func TestCheckReplicaMultiCluster(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Store: "inmemory"},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	})

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "c1", replica1)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2)
	assert.Error(t, err)

	// We should still accept from replica 1.
	err = c.checkReplica(context.Background(), "user", "c1", replica1)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1)
	assert.NoError(t, err)
}

func TestCheckReplicaMultiClusterTimeout(t *testing.T) {
	start := mtime.Now()
	replica1 := "replica1"
	replica2 := "replica2"

	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Store: "inmemory"},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	})
	assert.NoError(t, err)

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "c1", replica1)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2)
	assert.Error(t, err)

	// Accept a sample for replica1 in C2.
	mtime.NowForce(start.Add(500 * time.Millisecond))
	err = c.checkReplica(context.Background(), "user", "c2", replica1)
	assert.NoError(t, err)

	// Wait more than the timeout.
	mtime.NowForce(start.Add(1100 * time.Millisecond))

	// Accept a sample from c1/replica2.
	err = c.checkReplica(context.Background(), "user", "c1", replica2)
	assert.NoError(t, err)

	// We should still accept from c2/replica1 but reject from c1/replica1.
	err = c.checkReplica(context.Background(), "user", "c1", replica1)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1)
	assert.NoError(t, err)
}

// Test that writes only happen every update timeout.
func TestCheckReplicaUpdateTimeout(t *testing.T) {
	startTime := mtime.Now()
	replica := "r1"
	cluster := "c1"
	user := "user"

	codec := codec.Proto{Factory: ProtoReplicaDescFactory}
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Second,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	})
	assert.NoError(t, err)

	// Write the first time.
	mtime.NowForce(startTime)
	err = c.checkReplica(context.Background(), user, cluster, replica)

	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, startTime)
	cancel()
	assert.NoError(t, err)

	// Timestamp should not update here, since time has not advanced.
	err = c.checkReplica(context.Background(), user, cluster, replica)
	assert.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, startTime)
	cancel()
	assert.NoError(t, err)

	// Wait 500ms and the timestamp should still not update.
	updateTime := time.Unix(0, startTime.UnixNano()).Add(500 * time.Millisecond)
	mtime.NowForce(updateTime)

	err = c.checkReplica(context.Background(), user, cluster, replica)
	assert.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, startTime)
	cancel()
	assert.NoError(t, err)

	// Now we've waited > 1s, so the timestamp should update.
	updateTime = time.Unix(0, startTime.UnixNano()).Add(1100 * time.Millisecond)
	mtime.NowForce(updateTime)

	err = c.checkReplica(context.Background(), user, cluster, replica)
	assert.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, updateTime)
	cancel()
	assert.NoError(t, err)
}

// Test that writes only happen every write timeout.
func TestCheckReplicaMultiUser(t *testing.T) {
	start := mtime.Now()
	replica := "r1"
	cluster := "c1"
	user := "user"

	codec := codec.Proto{Factory: ProtoReplicaDescFactory}
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	})
	assert.NoError(t, err)

	// Write the first time for user 1.
	mtime.NowForce(start)
	err = c.checkReplica(ctxUser1, user, cluster, replica)
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(ctxUser1, time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, start)
	cancel()
	assert.NoError(t, err)

	// Write the first time for user 2.
	err = c.checkReplica(ctxUser2, user, cluster, replica)
	assert.NoError(t, err)
	ctx, cancel = context.WithTimeout(ctxUser2, time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, start)
	cancel()
	assert.NoError(t, err)

	// Now we've waited > 1s, so the timestamp should update.
	mtime.NowForce(start.Add(1100 * time.Millisecond))
	err = c.checkReplica(ctxUser1, user, cluster, replica)
	assert.NoError(t, err)
	ctx, cancel = context.WithTimeout(ctxUser1, time.Second)
	err = checkReplicaTimestamp(ctx, c, user, cluster, replica, mtime.Now())
	cancel()
	assert.NoError(t, err)
}

func TestCheckReplicaUpdateTimeoutJitter(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		updateTimeout     time.Duration
		updateJitter      time.Duration
		startTime         time.Time
		updateTime        time.Time
		expectedTimestamp time.Time
	}{
		"should not refresh the replica if the update timeout is not expired yet (without jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      0,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(14, 0),
			expectedTimestamp: time.Unix(5, 0),
		},
		"should refresh the replica if the update timeout is expired (without jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      0,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(15, 0),
			expectedTimestamp: time.Unix(15, 0),
		},
		"should not refresh the replica if the update timeout is not expired yet (with jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      2 * time.Second,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(16, 0),
			expectedTimestamp: time.Unix(5, 0),
		},
		"should refresh the replica if the update timeout is expired (with jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      2 * time.Second,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(17, 0),
			expectedTimestamp: time.Unix(17, 0),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init HA tracker
			codec := codec.Proto{Factory: ProtoReplicaDescFactory}
			mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
			c, err := newClusterTracker(HATrackerConfig{
				EnableHATracker:        true,
				KVStore:                kv.Config{Mock: mock},
				UpdateTimeout:          testData.updateTimeout,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        time.Second,
			})
			assert.NoError(t, err)

			// Init context used by the test
			ctx, cancel := context.WithTimeout(ctxUser1, time.Second)
			defer cancel()

			// Override the jitter so that it's not based on a random value
			// we can't control in tests
			c.updateTimeoutJitter = testData.updateJitter

			// Init the replica in the KV Store
			mtime.NowForce(testData.startTime)
			err = c.checkReplica(ctx, "user1", "cluster", "replica-1")
			require.NoError(t, err)
			err = checkReplicaTimestamp(ctx, c, "user1", "cluster", "replica-1", testData.startTime)
			assert.NoError(t, err)

			// Refresh the replica in the KV Store
			mtime.NowForce(testData.updateTime)
			err = c.checkReplica(ctx, "user1", "cluster", "replica-1")
			require.NoError(t, err)

			// Assert on the the received timestamp
			err = checkReplicaTimestamp(ctx, c, "user1", "cluster", "replica-1", testData.expectedTimestamp)
			assert.NoError(t, err)
		})
	}
}

func TestFindHALabels(t *testing.T) {
	replicaLabel, clusterLabel := "replica", "cluster"
	type expectedOutput struct {
		cluster string
		replica string
	}
	cases := []struct {
		labelsIn []client.LabelAdapter
		expected expectedOutput
	}{
		{
			[]client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: replicaLabel, Value: "1"},
			},
			expectedOutput{cluster: "", replica: "1"},
		},
		{
			[]client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: clusterLabel, Value: "cluster-2"},
			},
			expectedOutput{cluster: "cluster-2", replica: ""},
		},
		{
			[]client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: replicaLabel, Value: "3"},
				{Name: clusterLabel, Value: "cluster-3"},
			},
			expectedOutput{cluster: "cluster-3", replica: "3"},
		},
	}

	for _, c := range cases {
		cluster, replica := findHALabels(replicaLabel, clusterLabel, c.labelsIn)
		assert.Equal(t, c.expected.cluster, cluster)
		assert.Equal(t, c.expected.replica, replica)
	}
}
