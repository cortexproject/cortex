package querier

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/tls"
)

func TestBlocksStoreReplicationSet_GetClientsFor(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)

	tests := map[string]struct {
		replicationFactor int
		setup             func(*ring.Desc)
		queryBlocks       []ulid.ULID
		exclude           map[ulid.ULID][]string
		expectedClients   map[string][]ulid.ULID
		expectedErr       error
	}{
		"single instance in the ring with replication factor = 1": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"single instance in the ring with replication factor = 1 but excluded": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"single instance in the ring with replication factor = 1 but excluded for non queried block": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"single instance in the ring with replication factor = 2": {
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"multiple instances in the ring with each requested block belonging to a different store-gateway and replication factor = 1": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.3": {block3},
				"127.0.0.4": {block4},
			},
		},
		"multiple instances in the ring with each requested block belonging to a different store-gateway and replication factor = 1 but excluded": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.3"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block3.String()),
		},
		"multiple instances in the ring with each requested block belonging to a different store-gateway and replication factor = 2": {
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.3": {block3},
				"127.0.0.4": {block4},
			},
		},
		"multiple instances in the ring with multiple requested blocks belonging to the same store-gateway and replication factor = 2": {
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block3Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block2, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block4},
				"127.0.0.2": {block2, block3},
			},
		},
		"multiple instances in the ring with each requested block belonging to a different store-gateway and replication factor = 2 and some blocks excluded but with replacement available": {
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.3"},
				block1: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1},
				"127.0.0.4": {block3, block4},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			// Setup the ring state.
			ringStore := consul.NewInMemoryClient(ring.GetCodec())
			require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setup(d)
				return d, true, nil
			}))

			ringCfg := ring.Config{}
			flagext.DefaultValues(&ringCfg)
			ringCfg.ReplicationFactor = testData.replicationFactor

			r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, &storegateway.BlocksReplicationStrategy{})
			require.NoError(t, err)

			reg := prometheus.NewPedanticRegistry()
			s, err := newBlocksStoreReplicationSet(r, tls.ClientConfig{}, log.NewNopLogger(), reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, s))
			defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

			// Wait until the ring client has initialised the state.
			test.Poll(t, time.Second, true, func() interface{} {
				all, err := r.GetAll(ring.Read)
				return err == nil && len(all.Ingesters) > 0
			})

			clients, err := s.GetClientsFor(testData.queryBlocks, testData.exclude)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				assert.Equal(t, testData.expectedClients, getStoreGatewayClientAddrs(clients))

				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
					# TYPE cortex_storegateway_clients gauge
					cortex_storegateway_clients{client="querier"} %d
				`, len(testData.expectedClients))), "cortex_storegateway_clients"))
			}
		})
	}
}

func getStoreGatewayClientAddrs(clients map[BlocksStoreClient][]ulid.ULID) map[string][]ulid.ULID {
	addrs := map[string][]ulid.ULID{}
	for c, blockIDs := range clients {
		addrs[c.RemoteAddress()] = blockIDs
	}
	return addrs
}
