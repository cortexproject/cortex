// +build requires_docker

package main

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
	TableManager
	AlertManager
	Ruler
)

var (
	// Service-specific metrics prefixes which shouldn't be used by any other service.
	serviceMetricsPrefixes = map[ServiceType][]string{
		Distributor:   []string{},
		Ingester:      []string{"!cortex_ingester_client", "cortex_ingester"}, // The metrics prefix cortex_ingester_client may be used by other components so we ignore it.
		Querier:       []string{},
		QueryFrontend: []string{"cortex_frontend", "cortex_query_frontend"},
		TableManager:  []string{},
		AlertManager:  []string{"cortex_alertmanager"},
		Ruler:         []string{},
	}

	// Blacklisted metrics prefixes across any Cortex service.
	blacklistedMetricsPrefixes = []string{
		"cortex_alert_manager", // It should be "cortex_alertmanager"
	}
)

func assertServiceMetricsPrefixes(t *testing.T, serviceType ServiceType, service *e2ecortex.CortexService) {
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

			assert.NotRegexp(t, "^"+prefix, metricLine, "service: %s", service.Name())
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
