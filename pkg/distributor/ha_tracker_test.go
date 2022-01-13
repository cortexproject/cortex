package distributor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func checkReplicaTimestamp(t *testing.T, duration time.Duration, c *haTracker, user, cluster, replica string, expected time.Time) {
	key := fmt.Sprintf("%s/%s", user, cluster)

	// Round the expected timestamp with milliseconds precision
	// to match "received at" precision
	expected = expected.Truncate(time.Millisecond)

	test.Poll(t, duration, nil, func() interface{} {
		c.electedLock.RLock()
		r := c.elected[key]
		c.electedLock.RUnlock()

		if r.GetReplica() != replica {
			return fmt.Errorf("replicas did not match: %s != %s", r.GetReplica(), replica)
		}
		if r.GetDeletedAt() > 0 {
			return fmt.Errorf("replica is marked for deletion")
		}
		if !timestamp.Time(r.GetReceivedAt()).Equal(expected) {
			return fmt.Errorf("timestamps did not match: %+v != %+v", timestamp.Time(r.GetReceivedAt()), expected)
		}

		return nil
	})
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

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Millisecond * 2,
	}, trackerLimits{maxClusters: 100}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	// Write the first time.
	now := time.Now()

	err = c.checkReplica(context.Background(), "user", cluster, replica, now)
	assert.NoError(t, err)

	// Check to see if the value in the trackers cache is correct.
	checkReplicaTimestamp(t, time.Second, c, "user", cluster, replica, now)
}

func TestCheckReplicaOverwriteTimeout(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Store: "inmemory"},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "test", replica1, now)
	assert.NoError(t, err)

	// Throw away a sample from replica2.
	err = c.checkReplica(context.Background(), "user", "test", replica2, now)
	assert.Error(t, err)

	// Wait more than the overwrite timeout.
	now = now.Add(1100 * time.Millisecond)

	// Accept from replica 2, this should overwrite the saved replica of replica 1.
	err = c.checkReplica(context.Background(), "user", "test", replica2, now)
	assert.NoError(t, err)

	// We timed out accepting samples from replica 1 and should now reject them.
	err = c.checkReplica(context.Background(), "user", "test", replica1, now)
	assert.Error(t, err)
}

func TestCheckReplicaMultiCluster(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	reg := prometheus.NewPedanticRegistry()
	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Store: "inmemory"},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2, now)
	assert.Error(t, err)

	// We should still accept from replica 1.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now)
	assert.NoError(t, err)

	// We expect no CAS operation failures.
	metrics, err := reg.Gather()
	require.NoError(t, err)

	assert.Equal(t, uint64(0), util.GetSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "5.*"),
	}))
	assert.Greater(t, util.GetSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "2.*"),
	}), uint64(0))
}

func TestCheckReplicaMultiClusterTimeout(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	reg := prometheus.NewPedanticRegistry()
	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Store: "inmemory"},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2, now)
	assert.Error(t, err)

	// Accept a sample for replica1 in C2.
	now = now.Add(500 * time.Millisecond)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2, now)
	assert.Error(t, err)

	// Wait more than the failover timeout.
	now = now.Add(1100 * time.Millisecond)

	// Accept a sample from c1/replica2.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now)
	assert.NoError(t, err)

	// We should still accept from c2/replica1 but reject from c1/replica1.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now)
	assert.NoError(t, err)

	// We expect no CAS operation failures.
	metrics, err := reg.Gather()
	require.NoError(t, err)

	assert.Equal(t, uint64(0), util.GetSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "5.*"),
	}))
	assert.Greater(t, util.GetSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "2.*"),
	}), uint64(0))
}

// Test that writes only happen every update timeout.
func TestCheckReplicaUpdateTimeout(t *testing.T) {
	replica := "r1"
	cluster := "c1"
	user := "user"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Second,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	// Write the first time.
	startTime := time.Now()
	err = c.checkReplica(context.Background(), user, cluster, replica, startTime)
	assert.NoError(t, err)

	checkReplicaTimestamp(t, time.Second, c, user, cluster, replica, startTime)

	// Timestamp should not update here, since time has not advanced.
	err = c.checkReplica(context.Background(), user, cluster, replica, startTime)
	assert.NoError(t, err)

	checkReplicaTimestamp(t, time.Second, c, user, cluster, replica, startTime)

	// Wait 500ms and the timestamp should still not update.
	updateTime := time.Unix(0, startTime.UnixNano()).Add(500 * time.Millisecond)

	err = c.checkReplica(context.Background(), user, cluster, replica, updateTime)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, user, cluster, replica, startTime)

	// Now we've waited > 1s, so the timestamp should update.
	updateTime = time.Unix(0, startTime.UnixNano()).Add(1100 * time.Millisecond)

	err = c.checkReplica(context.Background(), user, cluster, replica, updateTime)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, user, cluster, replica, updateTime)
}

// Test that writes only happen every write timeout.
func TestCheckReplicaMultiUser(t *testing.T) {
	replica := "r1"
	cluster := "c1"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          100 * time.Millisecond,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time for user 1.
	err = c.checkReplica(context.Background(), "user1", cluster, replica, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, "user1", cluster, replica, now)

	// Write the first time for user 2.
	err = c.checkReplica(context.Background(), "user2", cluster, replica, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, "user2", cluster, replica, now)

	// Now we've waited > 1s, so the timestamp should update.
	updated := now.Add(1100 * time.Millisecond)
	err = c.checkReplica(context.Background(), "user1", cluster, replica, updated)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, "user1", cluster, replica, updated)
	// No update for user2.
	checkReplicaTimestamp(t, time.Second, c, "user2", cluster, replica, now)
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
			kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			mock := kv.PrefixClient(kvStore, "prefix")
			c, err := newHATracker(HATrackerConfig{
				EnableHATracker:        true,
				KVStore:                kv.Config{Mock: mock},
				UpdateTimeout:          testData.updateTimeout,
				UpdateTimeoutJitterMax: 0,
				FailoverTimeout:        time.Second,
			}, trackerLimits{maxClusters: 100}, nil, log.NewNopLogger())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
			defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

			// Init context used by the test
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Override the jitter so that it's not based on a random value
			// we can't control in tests
			c.updateTimeoutJitter = testData.updateJitter

			// Init the replica in the KV Store
			err = c.checkReplica(ctx, "user1", "cluster", "replica-1", testData.startTime)
			require.NoError(t, err)
			checkReplicaTimestamp(t, time.Second, c, "user1", "cluster", "replica-1", testData.startTime)

			// Refresh the replica in the KV Store
			err = c.checkReplica(ctx, "user1", "cluster", "replica-1", testData.updateTime)
			require.NoError(t, err)

			// Assert on the the received timestamp
			checkReplicaTimestamp(t, time.Second, c, "user1", "cluster", "replica-1", testData.expectedTimestamp)
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
		labelsIn []cortexpb.LabelAdapter
		expected expectedOutput
	}{
		{
			[]cortexpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: replicaLabel, Value: "1"},
			},
			expectedOutput{cluster: "", replica: "1"},
		},
		{
			[]cortexpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: clusterLabel, Value: "cluster-2"},
			},
			expectedOutput{cluster: "cluster-2", replica: ""},
		},
		{
			[]cortexpb.LabelAdapter{
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
	const userID = "user"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	limits := trackerLimits{maxClusters: 2}

	t1, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          time.Second,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, limits, nil, log.NewNopLogger())

	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), t1))
	defer services.StopAndAwaitTerminated(context.Background(), t1) //nolint:errcheck

	now := time.Now()

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "a", "a1", now))
	waitForClustersUpdate(t, 1, t1, userID)

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "b", "b1", now))
	waitForClustersUpdate(t, 2, t1, userID)

	assert.EqualError(t, t1.checkReplica(context.Background(), userID, "c", "c1", now), "too many HA clusters (limit: 2)")

	// Move time forward, and make sure that checkReplica for existing cluster works fine.
	now = now.Add(5 * time.Second) // higher than "update timeout"

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "b", "b2", now))
	waitForClustersUpdate(t, 2, t1, userID)

	// Mark cluster "a" for deletion (it was last updated 5 seconds ago)
	// We use seconds timestamp resolution here, to avoid cleaning up 'b'. (In KV store, we only store seconds).
	t1.cleanupOldReplicas(context.Background(), time.Unix(now.Unix(), 0))
	waitForClustersUpdate(t, 1, t1, userID)

	// Now adding cluster "c" works.
	assert.NoError(t, t1.checkReplica(context.Background(), userID, "c", "c1", now))
	waitForClustersUpdate(t, 2, t1, userID)

	// But yet another cluster doesn't.
	assert.EqualError(t, t1.checkReplica(context.Background(), userID, "a", "a2", now), "too many HA clusters (limit: 2)")

	now = now.Add(5 * time.Second)

	// clean all replicas
	t1.cleanupOldReplicas(context.Background(), now)
	waitForClustersUpdate(t, 0, t1, userID)

	// Now "a" works again.
	assert.NoError(t, t1.checkReplica(context.Background(), userID, "a", "a1", now))
	waitForClustersUpdate(t, 1, t1, userID)
}

func waitForClustersUpdate(t *testing.T, expected int, tr *haTracker, userID string) {
	t.Helper()
	test.Poll(t, 2*time.Second, expected, func() interface{} {
		tr.electedLock.RLock()
		defer tr.electedLock.RUnlock()

		return len(tr.clusters[userID])
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

func TestHATracker_MetricsCleanup(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	tr, err := newHATracker(HATrackerConfig{EnableHATracker: false}, nil, reg, log.NewNopLogger())
	require.NoError(t, err)

	metrics := []string{
		"cortex_ha_tracker_elected_replica_changes_total",
		"cortex_ha_tracker_elected_replica_timestamp_seconds",
		"cortex_ha_tracker_kv_store_cas_total",
	}

	tr.electedReplicaChanges.WithLabelValues("userA", "cluster1").Add(5)
	tr.electedReplicaChanges.WithLabelValues("userA", "cluster2").Add(8)
	tr.electedReplicaChanges.WithLabelValues("userB", "cluster").Add(10)
	tr.electedReplicaTimestamp.WithLabelValues("userA", "cluster1").Add(5)
	tr.electedReplicaTimestamp.WithLabelValues("userA", "cluster2").Add(8)
	tr.electedReplicaTimestamp.WithLabelValues("userB", "cluster").Add(10)
	tr.kvCASCalls.WithLabelValues("userA", "cluster1").Add(5)
	tr.kvCASCalls.WithLabelValues("userA", "cluster2").Add(8)
	tr.kvCASCalls.WithLabelValues("userB", "cluster").Add(10)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_changes_total The total number of times the elected replica has changed for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_changes_total counter
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster",user="userB"} 10
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster1",user="userA"} 5
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster2",user="userA"} 8

		# HELP cortex_ha_tracker_elected_replica_timestamp_seconds The timestamp stored for the currently elected replica, from the KVStore.
		# TYPE cortex_ha_tracker_elected_replica_timestamp_seconds gauge
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster",user="userB"} 10
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster1",user="userA"} 5
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster2",user="userA"} 8

		# HELP cortex_ha_tracker_kv_store_cas_total The total number of CAS calls to the KV store for a user ID/cluster.
		# TYPE cortex_ha_tracker_kv_store_cas_total counter
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster",user="userB"} 10
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster1",user="userA"} 5
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster2",user="userA"} 8
	`), metrics...))

	tr.cleanupHATrackerMetricsForUser("userA")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_changes_total The total number of times the elected replica has changed for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_changes_total counter
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster",user="userB"} 10

		# HELP cortex_ha_tracker_elected_replica_timestamp_seconds The timestamp stored for the currently elected replica, from the KVStore.
		# TYPE cortex_ha_tracker_elected_replica_timestamp_seconds gauge
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster",user="userB"} 10

		# HELP cortex_ha_tracker_kv_store_cas_total The total number of CAS calls to the KV store for a user ID/cluster.
		# TYPE cortex_ha_tracker_kv_store_cas_total counter
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster",user="userB"} 10
	`), metrics...))
}

func TestCheckReplicaCleanup(t *testing.T) {
	replica := "r1"
	cluster := "c1"
	userID := "user"
	ctx := user.InjectOrgID(context.Background(), userID)

	reg := prometheus.NewPedanticRegistry()

	kvStore, closer := consul.NewInMemoryClient(GetReplicaDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHATracker(HATrackerConfig{
		EnableHATracker:        true,
		KVStore:                kv.Config{Mock: mock},
		UpdateTimeout:          1 * time.Second,
		UpdateTimeoutJitterMax: 0,
		FailoverTimeout:        time.Second,
	}, trackerLimits{maxClusters: 100}, reg, util_log.Logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	err = c.checkReplica(context.Background(), userID, cluster, replica, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, userID, cluster, replica, now)

	// Replica is not marked for deletion yet.
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, true, true, false)
	checkUserClusters(t, time.Second, c, userID, 1)

	// This will mark replica for deletion (with time.Now())
	c.cleanupOldReplicas(ctx, now.Add(1*time.Second))

	// Verify marking for deletion.
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, false, true, true)
	checkUserClusters(t, time.Second, c, userID, 0)

	// This will "revive" the replica.
	now = time.Now()
	err = c.checkReplica(context.Background(), userID, cluster, replica, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, time.Second, c, userID, cluster, replica, now) // This also checks that entry is not marked for deletion.
	checkUserClusters(t, time.Second, c, userID, 1)

	// This will mark replica for deletion again (with new time.Now())
	c.cleanupOldReplicas(ctx, now.Add(1*time.Second))
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, false, true, true)
	checkUserClusters(t, time.Second, c, userID, 0)

	// Delete entry marked for deletion completely.
	c.cleanupOldReplicas(ctx, time.Now().Add(5*time.Second))
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, false, false, false)
	checkUserClusters(t, time.Second, c, userID, 0)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total Number of elected replicas marked for deletion.
		# TYPE cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total counter
		cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total 2

		# HELP cortex_ha_tracker_replicas_cleanup_deleted_total Number of elected replicas deleted from KV store.
		# TYPE cortex_ha_tracker_replicas_cleanup_deleted_total counter
		cortex_ha_tracker_replicas_cleanup_deleted_total 1

		# HELP cortex_ha_tracker_replicas_cleanup_delete_failed_total Number of elected replicas that failed to be marked for deletion, or deleted.
		# TYPE cortex_ha_tracker_replicas_cleanup_delete_failed_total counter
		cortex_ha_tracker_replicas_cleanup_delete_failed_total 0
	`), "cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total",
		"cortex_ha_tracker_replicas_cleanup_deleted_total",
		"cortex_ha_tracker_replicas_cleanup_delete_failed_total",
	))
}

func checkUserClusters(t *testing.T, duration time.Duration, c *haTracker, user string, expectedClusters int) {
	t.Helper()
	test.Poll(t, duration, nil, func() interface{} {
		c.electedLock.RLock()
		cl := len(c.clusters[user])
		c.electedLock.RUnlock()

		if cl != expectedClusters {
			return fmt.Errorf("expected clusters: %d, got %d", expectedClusters, cl)
		}

		return nil
	})
}

func checkReplicaDeletionState(t *testing.T, duration time.Duration, c *haTracker, user, cluster string, expectedExistsInMemory, expectedExistsInKV, expectedMarkedForDeletion bool) {
	key := fmt.Sprintf("%s/%s", user, cluster)

	test.Poll(t, duration, nil, func() interface{} {
		c.electedLock.RLock()
		_, exists := c.elected[key]
		c.electedLock.RUnlock()

		if exists != expectedExistsInMemory {
			return fmt.Errorf("exists in memory: expected=%v, got=%v", expectedExistsInMemory, exists)
		}

		return nil
	})

	val, err := c.client.Get(context.Background(), key)
	require.NoError(t, err)

	existsInKV := val != nil
	require.Equal(t, expectedExistsInKV, existsInKV, "exists in KV")

	if val != nil {
		markedForDeletion := val.(*ReplicaDesc).DeletedAt > 0
		require.Equal(t, expectedMarkedForDeletion, markedForDeletion, "KV entry marked for deletion")
	}
}
