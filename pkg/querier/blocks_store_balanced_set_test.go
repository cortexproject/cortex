package querier

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/tls"
)

func TestBlocksStoreBalancedSet_GetClientsFor(t *testing.T) {
	const numGets = 1000
	serviceAddrs := []string{"127.0.0.1", "127.0.0.2"}

	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	s := newBlocksStoreBalancedSet(serviceAddrs, tls.ClientConfig{}, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

	// Call the GetClientsFor() many times to measure the distribution
	// of returned clients (we expect an even distribution).
	clientsCount := map[string]int{}

	for i := 0; i < numGets; i++ {
		clients, err := s.GetClientsFor(nil)
		require.NoError(t, err)

		addrs := getStoreGatewayClientAddrs(clients)
		require.Len(t, addrs, 1)

		clientsCount[addrs[0]] = clientsCount[addrs[0]] + 1
	}

	assert.Len(t, clientsCount, len(serviceAddrs))
	for addr, count := range clientsCount {
		// Ensure that the number of times each client is returned is above
		// the 80% of the perfect even distribution.
		assert.Greaterf(t, count, int((float64(numGets)/float64(len(serviceAddrs)))*0.8), "service address: %s", addr)
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_storegateway_client_dns_failures_total The number of DNS lookup failures
		# TYPE cortex_storegateway_client_dns_failures_total counter
		cortex_storegateway_client_dns_failures_total 0
		# HELP cortex_storegateway_client_dns_lookups_total The number of DNS lookups resolutions attempts
		# TYPE cortex_storegateway_client_dns_lookups_total counter
		cortex_storegateway_client_dns_lookups_total 0
		# HELP cortex_storegateway_client_dns_provider_results The number of resolved endpoints for each configured address
		# TYPE cortex_storegateway_client_dns_provider_results gauge
		cortex_storegateway_client_dns_provider_results{addr="127.0.0.1"} 1
		cortex_storegateway_client_dns_provider_results{addr="127.0.0.2"} 1
		# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
		# TYPE cortex_storegateway_clients gauge
		cortex_storegateway_clients{client="querier"} 2
	`)))
}
