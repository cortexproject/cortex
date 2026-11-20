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

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// Two ingesters per zone, the first owning 3/4 of its zone's hash space and the
// second 1/4. Tokens are unique across the ring: a token maps to one instance
// ring-wide, whatever its zone.
//
// Within a zone the owner of a key is the first token at or after it, wrapping,
// so the instance holding 0xC00000xx owns [0, 0xC00000xx] (3/4) and the one
// holding 0xFFFFFFxx owns the rest.
var ownershipLimitsZones = []struct {
	zone         string
	largeShareID string
	largeToken   uint32
	smallShareID string
	smallToken   uint32
}{
	{"zone-a", "ingester-a-1", 0xC0000000, "ingester-a-2", 0xFFFFFFFF},
	{"zone-b", "ingester-b-1", 0xC0000001, "ingester-b-2", 0xFFFFFFFE},
	{"zone-c", "ingester-c-1", 0xC0000002, "ingester-c-2", 0xFFFFFFFD},
}

const (
	// With a replication factor of 3 over 3 zones, every zone holds one copy of
	// every series, so an ingester should hold globalSeries * its share.
	ownershipLimitsGlobalSeries = 2000
	ownershipLimitsLargeShare   = 0.75
	ownershipLimitsSmallShare   = 0.25

	// Equal-share conversion: global / 6 ingesters * replication factor 3.
	ownershipLimitsEqualShare = ownershipLimitsGlobalSeries / 6 * 3

	// Below the tenant's global limit, so no ingester should reject a series
	// when limits follow ownership. The ingesters owning 3/4 of their zone
	// receive about 1200 series, which is over the equal share of 1000.
	ownershipLimitsPushedSeries = 1600

	// Pushed before the ownership shares are awaited, to give every ingester a
	// TSDB for the tenant: a share is only computed for tenants an ingester
	// holds data for. Enough that the ingesters owning 1/4 get one with
	// certainty, and far below every limit involved.
	ownershipLimitsSeedSeries = 100

	ownershipLimitsSeriesPerRequest = 100

	// The first tenant's shard covers both ingesters of every zone, so its shares
	// stay uneven.
	ownershipLimitsShardSize = 6

	// The second tenant's shard is one ingester per zone, so each of them owns
	// all of that tenant's hash space and may reach the tenant's whole global
	// limit. It fails if the shares are read from the full ring instead of the
	// tenant's subring, which would cap it at 3/4 of the limit.
	ownershipLimitsSmallShardSize = 3

	smallShardUserID = "ownership-limits-small-shard"

	ownershipLimitsRuntimeConfig = "ownership-limits-runtime.yaml"

	// The share is refreshed on a ticker, after a heartbeat has fed the
	// ingester's own copy of the ring.
	ownershipLimitsShareTimeout = 60 * time.Second
)

// ownershipLimitsSeriesLabels returns the distinguishing label value of each
// series, the same ones for every arm of the test.
//
// The values are random rather than sequential: Cortex hashes a series with
// FNV-1a over its label names and values, and that hash is noticeably skewed
// for a run of sequential integers. With "0".."1599" only 57% of the series
// fall in the first 3/4 of the token space instead of 75%, which is enough to
// hide the effect being tested. Random values land within half a percent of it.
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
// uneven shares of their zone, and that the equal-share conversion does not.
func TestIngesterRingOwnershipLimits(t *testing.T) {
	require.Greater(t, ownershipLimitsPushedSeries*ownershipLimitsLargeShare, float64(ownershipLimitsEqualShare),
		"the test only shows a difference if the large-share ingesters exceed the equal share")

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
			flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(ownershipLimitsShardSize)
			flags["-ingester.max-series-per-user"] = "0"
			flags["-ingester.max-series-per-metric"] = "0"
			flags["-ingester.max-global-series-per-user"] = strconv.Itoa(ownershipLimitsGlobalSeries)
			flags["-ingester.max-global-series-per-metric"] = "0"
			flags["-ingester.heartbeat-period"] = "1s"
			flags["-ingester.use-ring-ownership-for-limits"] = strconv.FormatBool(useOwnership)
			flags["-runtime-config.file"] = e2e.ContainerSharedDir + "/" + ownershipLimitsRuntimeConfig
			flags["-runtime-config.reload-period"] = "1s"

			// Only the second tenant gets the smaller shard.
			runtimeConfig := fmt.Sprintf("overrides:\n  %s:\n    ingestion_tenant_shard_size: %d\n", smallShardUserID, ownershipLimitsSmallShardSize)
			require.NoError(t, writeFileToSharedDir(s, ownershipLimitsRuntimeConfig, []byte(runtimeConfig)))

			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			var ingesters []*e2ecortex.CortexService
			for _, zone := range ownershipLimitsZones {
				for _, instance := range []struct {
					id    string
					token uint32
				}{
					{zone.largeShareID, zone.largeToken},
					{zone.smallShareID, zone.smallToken},
				} {
					// A tokens file pins the share each ingester owns, instead of
					// relying on randomly generated tokens.
					tokensPath := fmt.Sprintf("tokens-%s.json", instance.id)
					content, err := json.Marshal(ring.TokenFile{Tokens: ring.Tokens{instance.token}})
					require.NoError(t, err)
					require.NoError(t, writeFileToSharedDir(s, tokensPath, content))

					instanceFlags := mergeFlags(flags, map[string]string{
						"-ingester.availability-zone": zone.zone,
						"-ingester.tokens-file-path":  e2e.ContainerSharedDir + "/" + tokensPath,
						"-ingester.num-tokens":        "1",
					})
					ingesters = append(ingesters, e2ecortex.NewIngester(instance.id, e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), instanceFlags, ""))
				}
			}

			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			toStart := []e2e.Service{distributor}
			for _, ingester := range ingesters {
				toStart = append(toStart, ingester)
			}
			require.NoError(t, s.StartAndWaitReady(toStart...))

			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(float64(len(ingesters))), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			userID := "ownership-limits"
			client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
			require.NoError(t, err)

			wholeRingShardClient, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", smallShardUserID)
			require.NoError(t, err)

			seriesLabels := ownershipLimitsSeriesLabels()

			pushed, rejected := pushOwnershipLimitsSeries(t, client, seriesLabels, 0, ownershipLimitsSeedSeries)
			require.Zero(t, rejected, "the seed push is far below every limit")

			if useOwnership {
				waitForOwnershipShares(t, ingesters, userID)
			} else {
				// Give the ingesters the same chance to settle, so both arms push
				// against a ring that has stopped changing.
				time.Sleep(15 * time.Second)
			}

			morePushed, rejected := pushOwnershipLimitsSeries(t, client, seriesLabels, ownershipLimitsSeedSeries, ownershipLimitsPushedSeries)
			pushed += morePushed

			perIngesterSeries := map[string]float64{}
			for _, ingester := range ingesters {
				values, err := ingester.SumMetrics([]string{"cortex_ingester_memory_series"})
				require.NoError(t, err)
				perIngesterSeries[ingester.Name()] = values[0]
			}
			t.Logf("accepted=%d series, requests rejected=%d, series per ingester=%v", pushed, rejected, perIngesterSeries)

			// The second tenant owns its whole shard, so it may use its whole
			// global limit on one ingester per zone.
			smallShardPushed, smallShardRejected := pushOwnershipLimitsSeries(t, wholeRingShardClient, seriesLabels, 0, ownershipLimitsPushedSeries)
			t.Logf("small-shard tenant: accepted=%d series, requests rejected=%d", smallShardPushed, smallShardRejected)
			assert.Zero(t, smallShardRejected, "the small-shard tenant stays below its global limit")
			assert.Equal(t, ownershipLimitsPushedSeries, smallShardPushed)

			// Its shard really is one ingester per zone, so the tenant's series
			// are held by three of the six ingesters and each holds all of them.
			holders := map[string]float64{}
			for _, ingester := range ingesters {
				values, err := ingester.SumMetrics([]string{"cortex_ingester_memory_series_created_total"},
					e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", smallShardUserID)),
					e2e.SkipMissingMetrics)
				require.NoError(t, err)
				if len(values) == 1 && values[0] > 0 {
					holders[ingester.Name()] = values[0]
				}
			}
			t.Logf("small-shard tenant held by %v", holders)
			assert.Len(t, holders, ownershipLimitsSmallShardSize)
			for name, series := range holders {
				assert.Equal(t, float64(ownershipLimitsPushedSeries), series, "ingester %s", name)
			}

			if useOwnership {
				assert.Zero(t, rejected, "no series should be rejected below the tenant's global limit")
				assert.Equal(t, ownershipLimitsPushedSeries, pushed)

				// Every zone holds one copy of every series, split by ownership.
				for _, zone := range ownershipLimitsZones {
					assert.InDelta(t, float64(pushed)*ownershipLimitsLargeShare, perIngesterSeries[zone.largeShareID], float64(pushed)*0.03)
					assert.InDelta(t, float64(pushed)*ownershipLimitsSmallShare, perIngesterSeries[zone.smallShareID], float64(pushed)*0.03)
				}
				return
			}

			// The equal-share conversion gives every ingester the same limit, so
			// the ones owning 3/4 of their zone stop accepting series at the equal
			// share, well below the tenant's global limit.
			assert.Positive(t, rejected, "the equal-share conversion is expected to reject series")
			assert.Less(t, pushed, ownershipLimitsPushedSeries)
			for _, zone := range ownershipLimitsZones {
				assert.InDelta(t, float64(ownershipLimitsEqualShare), perIngesterSeries[zone.largeShareID], float64(ownershipLimitsEqualShare)*0.05,
					"%s should be capped at the equal share", zone.largeShareID)
			}
		})
	}
}

// waitForOwnershipShares waits until every ingester reports the share of the
// tenant's hash space it owns within its zone.
func waitForOwnershipShares(t *testing.T, ingesters []*e2ecortex.CortexService, userID string) {
	t.Helper()

	for i, ingester := range ingesters {
		expected := ownershipLimitsSmallShare
		if i%2 == 0 {
			expected = ownershipLimitsLargeShare
		}

		// Not WaitSumMetricsWithOptions with WaitMissingMetrics: that spins
		// without consuming a retry while the metric has no series yet.
		test.Poll(t, ownershipLimitsShareTimeout, true, func() any {
			values, err := ingester.SumMetrics([]string{"cortex_ingester_ring_ownership_share"},
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", userID)),
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
