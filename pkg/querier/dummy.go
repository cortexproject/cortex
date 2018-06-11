package querier

import (
	"net/url"

	"github.com/prometheus/prometheus/scrape"
)

// DummyTargetRetriever implements github.com/prometheus/prometheus/web/api/v1.targetRetriever.
type DummyTargetRetriever struct{}

// TargetsActive implements targetRetriever.
func (r DummyTargetRetriever) TargetsActive() []*scrape.Target { return nil }

// TargetsDropped implements targetRetriever.
func (r DummyTargetRetriever) TargetsDropped() []*scrape.Target { return nil }

// DummyAlertmanagerRetriever implements AlertmanagerRetriever.
type DummyAlertmanagerRetriever struct{}

// Alertmanagers implements AlertmanagerRetriever.
func (r DummyAlertmanagerRetriever) Alertmanagers() []*url.URL { return nil }

// DroppedAlertmanagers implements AlertmanagerRetriever.
func (r DummyAlertmanagerRetriever) DroppedAlertmanagers() []*url.URL { return nil }
