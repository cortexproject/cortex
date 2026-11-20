//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// Three ingesters per zone, with tokens pinned so that within its zone the
// first owns 1/2 of the hash space and the other two 1/4 each.
//
// Within a zone the owner of a key is the first token at or after it, wrapping.
// Tokens are unique across the ring: a token maps to one instance ring-wide,
// whatever its zone, so each zone's tokens are offset by one.
var ownershipLimitsZones = []string{"zone-a", "zone-b", "zone-c"}

func ownershipLimitsInstances() []struct {
	id     string
	zone   string
	tokens ring.Tokens
} {
	var out []struct {
		id     string
		zone   string
		tokens ring.Tokens
	}
	for i, zone := range ownershipLimitsZones {
		offset := uint32(i)
		for suffix, token := range map[int]uint32{
			1: 0x80000000 + offset, // owns [0xC0000000+, 0x80000000] wrapping: 1/2
			2: 0xC0000000 + offset, // owns (0x80000000, 0xC0000000]: 1/4
			3: 0xFFFFFFFF - offset, // owns (0xC0000000, 0xFFFFFFFF]: 1/4
		} {
			out = append(out, struct {
				id     string
				zone   string
				tokens ring.Tokens
			}{
				id:     fmt.Sprintf("ingester-%s-%d", zone[len(zone)-1:], suffix),
				zone:   zone,
				tokens: ring.Tokens{token},
			})
		}
	}
	return out
}

const (
	// With a replication factor of 3 over 3 zones, every zone holds one copy of
	// every series, so an ingester should hold globalSeries * its share.
	ownershipLimitsGlobalSeries = 4000

	// Below the tenants' global limit, so no ingester should reject a series
	// when limits follow ownership, and above the equal share of the ingesters
	// owning the most of a zone, so the equal-share conversion rejects series
	// for both tenants. The margins either side are about six standard
	// deviations of how unevenly the series hash over the token space, which is
	// what sets the scale: a quarter of this many series leaves margins small
	// enough for that noise to decide the verdict.
	ownershipLimitsPushedSeries = 3200

	// Pushed before the ownership shares are awaited, to give every ingester a
	// TSDB for the tenant: a share is only computed for tenants an ingester
	// holds data for. Enough that the ingesters owning the least get one with
	// certainty, and far below every limit involved.
	ownershipLimitsSeedSeries = 100

	ownershipLimitsSeriesPerRequest = 100

	// The first tenant's shard is the whole ring.
	ownershipLimitsFullRingUser  = "ownership-limits"
	ownershipLimitsFullRingShard = 9

	// The second tenant's shard is two ingesters per zone, a real subset of the
	// nine. Its name is chosen so that the shard is the 1/2-share and one
	// 1/4-share ingester of every zone, which own 3/4 and 1/4 of the tenant's
	// hash space: shares that exist only in the subring. Reading them from the
	// whole ring instead would cap the tenant at 1/2 of its global limit.
	ownershipLimitsSubringUser  = "tenant-21"
	ownershipLimitsSubringShard = 6

	ownershipLimitsRuntimeConfig = "ownership-limits-runtime.yaml"

	// The share is refreshed on a ticker, after a heartbeat has fed the
	// ingester's own copy of the ring.
	ownershipLimitsShareTimeout = 60 * time.Second
)

// ownershipLimitsSeriesLabels returns the distinguishing label value of each
// series, the same ones for every tenant and every arm of the test.
//
// The values are random rather than sequential: Cortex hashes a series with
// FNV-1a over its label names and values, and that hash is noticeably skewed
// for a run of sequential integers. With a sequential run only 57% of the series
// fall in the first 3/4 of the token space instead of 75%, which is enough to
// hide the effect being tested. Random values land within a percent of it.
func ownershipLimitsSeriesLabels() []string {
	rnd := rand.New(rand.NewSource(0))
	values := make([]string, ownershipLimitsPushedSeries)
	for i := range values {
		values[i] = strconv.Itoa(rnd.Int())
	}
	return values
}

// TestIngesterRingOwnershipLimits asserts that -ingester.use-ring-ownership-for-limits
// lets a tenant reach its global series limit on a ring whose instances own
// uneven shares of their zone, that the equal-share conversion does not, and
// that a tenant's shares come from its own shard rather than the whole ring.
func TestIngesterRingOwnershipLimits(t *testing.T) {
	instances := ownershipLimitsInstances()

	// Shares a tenant's ingesters should see, derived from the same ring the
	// cluster is given. Fails the test if the layout above stops producing the
	// skew the assertions below rely on.
	sharesByUser := map[string]map[string]float64{
		ownershipLimitsFullRingUser: ownershipLimitsShares(t, instances, ownershipLimitsFullRingUser, ownershipLimitsFullRingShard),
		ownershipLimitsSubringUser:  ownershipLimitsShares(t, instances, ownershipLimitsSubringUser, ownershipLimitsSubringShard),
	}
	require.Len(t, sharesByUser[ownershipLimitsSubringUser], ownershipLimitsSubringShard,
		"the subring tenant's shard should not be the whole ring")

	// Pushing below the global limit means ownership-based limits reject
	// nothing, whatever the shares are.
	require.Less(t, ownershipLimitsPushedSeries, ownershipLimitsGlobalSeries)

	// The arms can only differ if the ingester owning the most of a zone goes
	// past the equal share.
	for user, shares := range sharesByUser {
		maxShare := 0.0
		for _, share := range shares {
			maxShare = max(maxShare, share)
		}
		require.Greater(t, float64(ownershipLimitsPushedSeries)*maxShare, float64(ownershipLimitsEqualShare(len(shares))),
			"tenant %s does not push any ingester past the equal share", user)
	}

	for _, useOwnership := range []bool{false, true} {
		name := "equal share"
		if useOwnership {
			name = "ring ownership"
		}

		t.Run(name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := BlocksStorageFlags()
			flags["-distributor.replication-factor"] = "3"
			flags["-distributor.zone-awareness-enabled"] = "true"
			flags["-distributor.shard-by-all-labels"] = "true"
			flags["-distributor.sharding-strategy"] = "shuffle-sharding"
			flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(ownershipLimitsFullRingShard)
			flags["-ingester.max-series-per-user"] = "0"
			flags["-ingester.max-series-per-metric"] = "0"
			flags["-ingester.max-global-series-per-user"] = strconv.Itoa(ownershipLimitsGlobalSeries)
			flags["-ingester.max-global-series-per-metric"] = "0"
			flags["-ingester.heartbeat-period"] = "1s"
			flags["-ingester.use-ring-ownership-for-limits"] = strconv.FormatBool(useOwnership)
			flags["-runtime-config.file"] = e2e.ContainerSharedDir + "/" + ownershipLimitsRuntimeConfig
			flags["-runtime-config.reload-period"] = "1s"

			// Only the second tenant gets the smaller shard.
			runtimeConfig := fmt.Sprintf("overrides:\n  %s:\n    ingestion_tenant_shard_size: %d\n",
				ownershipLimitsSubringUser, ownershipLimitsSubringShard)
			require.NoError(t, writeFileToSharedDir(s, ownershipLimitsRuntimeConfig, []byte(runtimeConfig)))

			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			ingesters := map[string]*e2ecortex.CortexService{}
			toStart := []e2e.Service{}
			for _, instance := range instances {
				// A tokens file pins the share each ingester owns, instead of
				// relying on randomly generated tokens.
				tokensPath := fmt.Sprintf("tokens-%s.json", instance.id)
				content, err := json.Marshal(ring.TokenFile{Tokens: instance.tokens})
				require.NoError(t, err)
				require.NoError(t, writeFileToSharedDir(s, tokensPath, content))

				instanceFlags := mergeFlags(flags, map[string]string{
					"-ingester.availability-zone": instance.zone,
					"-ingester.tokens-file-path":  e2e.ContainerSharedDir + "/" + tokensPath,
					"-ingester.num-tokens":        "1",
				})
				ingesters[instance.id] = e2ecortex.NewIngester(instance.id, e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), instanceFlags, "")
				toStart = append(toStart, ingesters[instance.id])
			}

			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(append(toStart, distributor)...))

			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(float64(len(instances))), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			seriesLabels := ownershipLimitsSeriesLabels()

			for _, user := range []string{ownershipLimitsFullRingUser, ownershipLimitsSubringUser} {
				shares := sharesByUser[user]
				equalShare := ownershipLimitsEqualShare(len(shares))

				client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", user)
				require.NoError(t, err)

				pushed, rejected := pushOwnershipLimitsSeries(t, client, seriesLabels, 0, ownershipLimitsSeedSeries)
				require.Zero(t, rejected, "the seed push is far below every limit")

				if useOwnership {
					waitForOwnershipShares(t, ingesters, user, shares)
				} else {
					// Give the ingesters the same chance to settle, so both arms
					// push against a ring that has stopped changing.
					time.Sleep(15 * time.Second)
				}

				morePushed, rejected := pushOwnershipLimitsSeries(t, client, seriesLabels, ownershipLimitsSeedSeries, ownershipLimitsPushedSeries)
				pushed += morePushed

				held := map[string]float64{}
				for id, ingester := range ingesters {
					values, err := ingester.SumMetrics([]string{"cortex_ingester_memory_series_created_total"},
						e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", user)),
						e2e.SkipMissingMetrics)
					require.NoError(t, err)
					if len(values) == 1 && values[0] > 0 {
						held[id] = values[0]
					}
				}
				t.Logf("%s: accepted=%d series, requests rejected=%d, equal share=%d, series per ingester=%v",
					user, pushed, rejected, equalShare, held)

				// Only the ingesters in the tenant's shard hold its series.
				assert.Len(t, held, len(shares))
				for id := range held {
					assert.Contains(t, shares, id, "%s is not in %s's shard", id, user)
				}

				if useOwnership {
					assert.Zero(t, rejected, "no series should be rejected below %s's global limit", user)
					assert.Equal(t, ownershipLimitsPushedSeries, pushed)

					// Every zone holds one copy of every series, split by ownership.
					for id, share := range shares {
						assert.InDelta(t, float64(pushed)*share, held[id], float64(pushed)*0.06, "ingester %s", id)
					}
					continue
				}

				// The equal-share conversion gives every ingester in the shard
				// the same limit, so the ones owning more than their share of a
				// zone stop accepting series well below the tenant's global limit.
				assert.Positive(t, rejected, "the equal-share conversion is expected to reject series")
				assert.Less(t, pushed, ownershipLimitsPushedSeries)
				for id, share := range shares {
					if float64(ownershipLimitsPushedSeries)*share <= float64(equalShare) {
						continue
					}
					assert.InDelta(t, float64(equalShare), held[id], float64(equalShare)*0.05,
						"%s should be capped at the equal share", id)
				}
			}
		})
	}
}

// ownershipLimitsEqualShare is the local limit the equal-share conversion gives
// every ingester of a tenant whose shard holds shardSize instances.
func ownershipLimitsEqualShare(shardSize int) int {
	return int(float64(ownershipLimitsGlobalSeries) / float64(shardSize) * float64(len(ownershipLimitsZones)))
}

// ownershipLimitsShares returns the share of the tenant's hash space each
// ingester of its shard owns within its zone, read from a ring built from the
// same instances the cluster runs.
func ownershipLimitsShares(t *testing.T, instances []struct {
	id     string
	zone   string
	tokens ring.Tokens
}, user string, shardSize int) map[string]float64 {
	t.Helper()

	desc := ring.NewDesc()
	for _, instance := range instances {
		desc.AddIngester(instance.id, instance.id, instance.zone, instance.tokens, ring.ACTIVE, time.Time{})
	}

	kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	r, err := ring.NewDescFedRing(ring.Config{
		HeartbeatTimeout:     time.Hour,
		ReplicationFactor:    len(ownershipLimitsZones),
		ZoneAwarenessEnabled: true,
	}, "ownership-limits", "ring", kvClient, log.NewNopLogger())
	require.NoError(t, err)
	r.UpdateDesc(desc)

	var scoped ring.ReadRing = r
	if shardSize < len(instances) {
		scoped = r.ShuffleShard(user, shardSize)
	}

	shares := map[string]float64{}
	for id, share := range scoped.(ring.ZonalOwnershipRing).ZonalOwnership(ring.Write) {
		if share > 0 {
			shares[id] = share
		}
	}
	return shares
}

// waitForOwnershipShares waits until every ingester in the tenant's shard
// reports the share of its hash space it owns within its zone.
func waitForOwnershipShares(t *testing.T, ingesters map[string]*e2ecortex.CortexService, user string, shares map[string]float64) {
	t.Helper()

	for id, expected := range shares {
		ingester := ingesters[id]

		// Not WaitSumMetricsWithOptions with WaitMissingMetrics: that spins
		// without consuming a retry while the metric has no series yet.
		test.Poll(t, ownershipLimitsShareTimeout, true, func() any {
			values, err := ingester.SumMetrics([]string{"cortex_ingester_ring_ownership_share"},
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", user)),
				e2e.SkipMissingMetrics)
			if err != nil || len(values) != 1 {
				return false
			}
			return math.Abs(values[0]-expected) < 0.001
		})
	}
}

// pushOwnershipLimitsSeries pushes distinct series with indexes in [from, to)
// in batches, and returns how many were accepted and how many requests were
// rejected for exceeding a limit.
func pushOwnershipLimitsSeries(t *testing.T, client *e2ecortex.Client, seriesLabels []string, from, to int) (pushed, rejected int) {
	t.Helper()
	now := time.Now()

	for start := from; start < to; start += ownershipLimitsSeriesPerRequest {
		size := min(ownershipLimitsSeriesPerRequest, to-start)
		batch := make([]prompb.TimeSeries, 0, size)
		for i := start; i < start+size; i++ {
			series, _ := generateSeries("ownership_limits", now, prompb.Label{
				Name:  "cardinality",
				Value: seriesLabels[i],
			})
			batch = append(batch, series...)
		}

		res, err := client.Push(batch)
		require.NoError(t, err)

		switch res.StatusCode {
		case 200:
			pushed += size
		case 400:
			// A partial write: the series that fit were written and the rest
			// rejected. The series actually stored are read back from the
			// ingesters, so only count the request as having lost series.
			rejected++
		default:
			require.Failf(t, "unexpected status code pushing series", "got %d", res.StatusCode)
		}
	}
	return pushed, rejected
}
