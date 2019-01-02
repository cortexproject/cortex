package querier

import (
	"net/url"

	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
)

// DummyTargetRetriever implements github.com/prometheus/prometheus/web/api/v1.targetRetriever.
type DummyTargetRetriever struct{}

// TargetsActive implements targetRetriever.
func (DummyTargetRetriever) TargetsActive() []*scrape.Target { return nil }

// TargetsDropped implements targetRetriever.
func (DummyTargetRetriever) TargetsDropped() []*scrape.Target { return nil }

// DummyAlertmanagerRetriever implements AlertmanagerRetriever.
type DummyAlertmanagerRetriever struct{}

// Alertmanagers implements AlertmanagerRetriever.
func (DummyAlertmanagerRetriever) Alertmanagers() []*url.URL { return nil }

// DroppedAlertmanagers implements AlertmanagerRetriever.
func (DummyAlertmanagerRetriever) DroppedAlertmanagers() []*url.URL { return nil }

// DummyRulesRetriever implements RulesRetriever.
type DummyRulesRetriever struct{}

// RuleGroups implements RulesRetriever.
func (DummyRulesRetriever) RuleGroups() []*rules.Group {
	return nil
}

// AlertingRules implements RulesRetriever.
func (DummyRulesRetriever) AlertingRules() []*rules.AlertingRule {
	return nil
}
