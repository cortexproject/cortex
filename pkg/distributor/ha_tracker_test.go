package distributor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/mtime"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
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
			}
			if !timestamp.Time(r.GetReceivedAt()).Equal(expected) {
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

func TestHATrackerConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfg         HATrackerConfig
		expectedErr error
	}{
		"should pass with default config": {
			cfg: func() HATrackerConfig {
				cfg := HATrackerConfig{}
				flagext.DefaultValues(&cfg)

				return cfg
			}(),
			expectedErr: nil,
		},
		"should fail if max update timeout jitter is negative": {
			cfg: func() HATrackerConfig {
				cfg := HATrackerConfig{}
				flagext.DefaultValues(&cfg)
				cfg.UpdateTimeoutJitterMax = -1

				return cfg
			}(),
			expectedErr: errNegativeUpdateTimeoutJitterMax,
		},
		"should fail if failover timeout is < update timeout + jitter + 1 sec": {
			cfg: func() HATrackerConfig {
				cfg := HATrackerConfig{}
				flagext.DefaultValues(&cfg)
				cfg.FailoverTimeout = 5 * time.Second
				cfg.UpdateTimeout = 4 * time.Second
				cfg.UpdateTimeoutJitterMax = 2 * time.Second

				return cfg
			}(),
			expectedErr: fmt.Errorf(errInvalidFailoverTimeout, 5*time.Second, 7*time.Second),
		},
		"should pass if failover timeout is >= update timeout + jitter + 1 sec": {
			cfg: func() HATrackerConfig {
				cfg := HATrackerConfig{}
				flagext.DefaultValues(&cfg)
				cfg.FailoverTimeout = 7 * time.Second
				cfg.UpdateTimeout = 4 * time.Second
				cfg.UpdateTimeoutJitterMax = 2 * time.Second

				return cfg
			}(),
			expectedErr: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedErr, testData.cfg.Validate())
		})
	}
}

// Test that values are set in the HATracker after WatchPrefix has found it in the KVStore.
func TestWatchPrefixAssignment(t *testing.T) {
	cluster := "c1"
	replica := "r1"
	start := mtime.Now()

	codec := GetReplicaDescCodec()
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Millisecond * 2,
	}, trackerLimits{maxClusters: 100}, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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
	}, trackerLimits{maxClusters: 100}, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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
	}, trackerLimits{maxClusters: 100}, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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
	}, trackerLimits{maxClusters: 100}, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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

	codec := GetReplicaDescCodec()
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Second,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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

	codec := GetReplicaDescCodec()
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	c, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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
			codec := GetReplicaDescCodec()
			mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
			c, err := newClusterTracker(HATrackerConfig{
				EnableHATracker:        true,
				KVStore:                kv.Config{Mock: mock},
				UpdateTimeout:          testData.updateTimeout,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        time.Second,
			}, trackerLimits{maxClusters: 100}, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
			defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

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

func TestHATrackerConfig_ShouldCustomizePrefixDefaultValue(t *testing.T) {
	haConfig := HATrackerConfig{}
	ringConfig := ring.Config{}
	flagext.DefaultValues(&haConfig)
	flagext.DefaultValues(&ringConfig)

	assert.Equal(t, "ha-tracker/", haConfig.KVStore.Prefix)
	assert.NotEqual(t, haConfig.KVStore.Prefix, ringConfig.KVStore.Prefix)
}

func TestHAClustersLimit(t *testing.T) {
	defer mtime.NowReset()

	const userID = "user"

	codec := GetReplicaDescCodec()
	mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
	limits := trackerLimits{maxClusters: 2}

	t1, err := newClusterTracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Second,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, limits, nil)

	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), t1))
	defer services.StopAndAwaitTerminated(context.Background(), t1) //nolint:errcheck

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "a", "a1"))
	waitForClustersUpdate(t, 1, t1, userID)

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "b", "b1"))
	waitForClustersUpdate(t, 2, t1, userID)

	assert.EqualError(t, t1.checkReplica(context.Background(), userID, "c", "c1"), "too many HA clusters (limit: 2)")

	// Move time forward, and make sure that checkReplica for existing cluster works fine.
	mtime.NowForce(time.Now().Add(5 * time.Second)) // higher than "update timeout"

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "b", "b2"))
	waitForClustersUpdate(t, 2, t1, userID)
}

func waitForClustersUpdate(t *testing.T, expected int, tr *haTracker, userID string) {
	t.Helper()
	test.Poll(t, 2*time.Second, expected, func() interface{} {
		tr.electedLock.RLock()
		defer tr.electedLock.RUnlock()

		return tr.clusters[userID]
	})
}

func TestTooManyClustersError(t *testing.T) {
	var err error = tooManyClustersError{limit: 10}
	assert.True(t, errors.Is(err, tooManyClustersError{}))
	assert.True(t, errors.Is(err, &tooManyClustersError{}))

	err = &tooManyClustersError{limit: 20}
	assert.True(t, errors.Is(err, tooManyClustersError{}))
	assert.True(t, errors.Is(err, &tooManyClustersError{}))

	err = replicasNotMatchError{replica: "a", elected: "b"}
	assert.False(t, errors.Is(err, tooManyClustersError{}))
	assert.False(t, errors.Is(err, &tooManyClustersError{}))
}

func TestReplicasNotMatchError(t *testing.T) {
	var err error = replicasNotMatchError{replica: "a", elected: "b"}
	assert.True(t, errors.Is(err, replicasNotMatchError{}))
	assert.True(t, errors.Is(err, &replicasNotMatchError{}))

	err = &replicasNotMatchError{replica: "a", elected: "b"}
	assert.True(t, errors.Is(err, replicasNotMatchError{}))
	assert.True(t, errors.Is(err, &replicasNotMatchError{}))

	err = tooManyClustersError{limit: 10}
	assert.False(t, errors.Is(err, replicasNotMatchError{}))
	assert.False(t, errors.Is(err, &replicasNotMatchError{}))
}

type trackerLimits struct {
	maxClusters int
}

func (l trackerLimits) MaxHAClusters(_ string) int {
	return l.maxClusters
}
