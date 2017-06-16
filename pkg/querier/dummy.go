package querier

import "github.com/prometheus/prometheus/retrieval"

// DummyTargetRetriever implements TargetRetriever
type DummyTargetRetriever struct{}

func (r DummyTargetRetriever) Targets() []*retrieval.Target { return nil }

// DummyAlertmanagerRetriever implements AlertmanagerRetriever
type DummyAlertmanagerRetriever struct{}

func (r DummyAlertmanagerRetriever) Alertmanagers() []string { return nil }
