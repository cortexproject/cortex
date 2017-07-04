package querier

import "github.com/prometheus/prometheus/retrieval"

// DummyTargetRetriever implements TargetRetriever.
type DummyTargetRetriever struct{}

// Targets implements TargetRetriever.
func (r DummyTargetRetriever) Targets() []*retrieval.Target { return nil }

// DummyAlertmanagerRetriever implements AlertmanagerRetriever.
type DummyAlertmanagerRetriever struct{}

// Alertmanagers implements AlertmanagerRetriever.
func (r DummyAlertmanagerRetriever) Alertmanagers() []string { return nil }
