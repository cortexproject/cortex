// +build requires_docker

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2ecortex"
)

type ServiceType int

const (
	Distributor = iota
	Ingester
	Querier
	QueryFrontend
	QueryScheduler
	TableManager
	AlertManager
	Ruler
	StoreGateway
	Purger
)

var (
	// Service-specific metrics prefixes which shouldn't be used by any other service.
	serviceMetricsPrefixes = map[ServiceType][]string{
		Distributor: {},
		// The metrics prefix cortex_ingester_client may be used by other components so we ignore it.
		Ingester: {"!cortex_ingester_client", "cortex_ingester"},
		// The metrics prefixes cortex_querier_storegateway and cortex_querier_blocks may be used by other components so we ignore them.
		Querier:        {"!cortex_querier_storegateway", "!cortex_querier_blocks", "cortex_querier"},
		QueryFrontend:  {"cortex_frontend", "cortex_query_frontend"},
		QueryScheduler: {"cortex_query_scheduler"},
		TableManager:   {},
		AlertManager:   {"cortex_alertmanager"},
		Ruler:          {},
		// The metrics prefix cortex_storegateway_client may be used by other components so we ignore it.
		StoreGateway: {"!cortex_storegateway_client", "cortex_storegateway"},
		Purger:       {"cortex_purger"},
	}

	// Blacklisted metrics prefixes across any Cortex service.
	blacklistedMetricsPrefixes = []string{
		"cortex_alert_manager", // It should be "cortex_alertmanager"
		"cortex_store_gateway", // It should be "cortex_storegateway"
	}
)

func assertServiceMetricsPrefixes(t *testing.T, serviceType ServiceType, service *e2ecortex.CortexService) {
	if service == nil {
		return
	}

	metrics, err := service.Metrics()
	require.NoError(t, err)

	// Build the list of blacklisted metrics prefixes for this specific service.
	blacklist := getBlacklistedMetricsPrefixesByService(serviceType)

	// Ensure no metric name matches the blacklisted prefixes.
	for _, metricLine := range strings.Split(metrics, "\n") {
		metricLine = strings.TrimSpace(metricLine)
		if metricLine == "" || strings.HasPrefix(metricLine, "#") {
			continue
		}

		for _, prefix := range blacklist {
			// Skip the metric if it matches an ignored prefix.
			if prefix[0] == '!' && strings.HasPrefix(metricLine, prefix[1:]) {
				break
			}

			assert.NotRegexp(t, "^"+prefix, metricLine, "service: %s endpoint: %s", service.Name(), service.HTTPEndpoint())
		}
	}
}

func getBlacklistedMetricsPrefixesByService(serviceType ServiceType) []string {
	blacklist := append([]string{}, blacklistedMetricsPrefixes...)

	// Add any service-specific metrics prefix excluding the service itself.
	for t, prefixes := range serviceMetricsPrefixes {
		if t == serviceType {
			continue
		}

		blacklist = append(blacklist, prefixes...)
	}

	return blacklist
}
