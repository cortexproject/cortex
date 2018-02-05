package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/ring"
)

func TestReplicationStrategy(t *testing.T) {
	for i, tc := range []struct {
		RF, LiveIngesters, DeadIngesters       int
		ExpectedMinSuccess, ExpectedMaxFailure int
		ExpectedError                          string
	}{
		// Ensure it works for a single ingester, for local testing.
		{
			RF:                 1,
			LiveIngesters:      1,
			ExpectedMinSuccess: 1,
		},

		{
			RF:                 1,
			DeadIngesters:      1,
			ExpectedMinSuccess: 1,
			ExpectedError:      "at least 1 live ingesters required, could only find 0",
		},

		// Ensure it works for the default production config.
		{
			RF:                 3,
			LiveIngesters:      3,
			ExpectedMinSuccess: 2,
			ExpectedMaxFailure: 1,
		},

		{
			RF:                 3,
			LiveIngesters:      2,
			DeadIngesters:      1,
			ExpectedMinSuccess: 2,
			ExpectedMaxFailure: 1,
		},

		{
			RF:                 3,
			LiveIngesters:      1,
			DeadIngesters:      2,
			ExpectedMinSuccess: 2,
			ExpectedMaxFailure: 1,
			ExpectedError:      "at least 2 live ingesters required, could only find 1",
		},

		// Ensure it works when adding / removing nodes.

		// A node is joining or leaving, replica set expands.
		{
			RF:                 3,
			LiveIngesters:      4,
			ExpectedMinSuccess: 3,
			ExpectedMaxFailure: 1,
		},

		{
			RF:                 3,
			LiveIngesters:      2,
			DeadIngesters:      2,
			ExpectedMinSuccess: 3,
			ExpectedMaxFailure: 1,
			ExpectedError:      "at least 3 live ingesters required, could only find 2",
		},
	} {
		ingesters := []*ring.IngesterDesc{}
		for i := 0; i < tc.LiveIngesters; i++ {
			ingesters = append(ingesters, &ring.IngesterDesc{
				Timestamp: time.Now().Unix(),
			})
		}
		for i := 0; i < tc.DeadIngesters; i++ {
			ingesters = append(ingesters, &ring.IngesterDesc{})
		}

		r, err := ring.New(ring.Config{
			Mock:             ring.NewInMemoryKVClient(),
			HeartbeatTimeout: 100 * time.Second,
		})
		require.NoError(t, err)

		d := Distributor{
			cfg: Config{
				ReplicationFactor: tc.RF,
			},
			ring: r,
		}

		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			minSuccess, maxFailure, _, err := d.replicationStrategy(ingesters)
			assert.Equal(t, tc.ExpectedMinSuccess, minSuccess)
			assert.Equal(t, tc.ExpectedMaxFailure, maxFailure)
			if tc.ExpectedError != "" {
				assert.EqualError(t, err, tc.ExpectedError)
			}
		})
	}
}
