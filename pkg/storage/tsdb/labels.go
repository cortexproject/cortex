package tsdb

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	legacy_labels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/labels"
)

const (
	// TenantIDExternalLabel is the external label set when shipping blocks to the storage
	TenantIDExternalLabel = "__org_id__"
)

// FromLabelAdaptersToLabels converts []LabelAdapter to TSDB labels.Labels.
func FromLabelAdaptersToLabels(input []client.LabelAdapter) labels.Labels {
	result := make(labels.Labels, len(input))

	for i, l := range input {
		result[i] = labels.Label{
			Name:  l.Name,
			Value: l.Value,
		}
	}

	return result
}

// FromLabelsToLabelAdapters converts TSDB labels.labels to []LabelAdapter.
func FromLabelsToLabelAdapters(labels labels.Labels) []client.LabelAdapter {
	adapters := make([]client.LabelAdapter, 0, len(labels))

	for _, label := range labels {
		adapters = append(adapters, client.LabelAdapter(label))
	}

	return adapters
}

// FromLegacyLabelMatchersToMatchers converts legacy matchers to TSDB label matchers.
func FromLegacyLabelMatchersToMatchers(matchers []*legacy_labels.Matcher) ([]labels.Matcher, error) {
	converted := make([]labels.Matcher, 0, len(matchers))

	for _, m := range matchers {
		switch m.Type {
		case legacy_labels.MatchEqual:
			converted = append(converted, labels.NewEqualMatcher(m.Name, m.Value))
		case legacy_labels.MatchNotEqual:
			converted = append(converted, labels.Not(labels.NewEqualMatcher(m.Name, m.Value)))
		case legacy_labels.MatchRegexp:
			rm, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
			if err != nil {
				return nil, err
			}
			converted = append(converted, rm)
		case legacy_labels.MatchNotRegexp:
			rm, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
			if err != nil {
				return nil, err
			}
			converted = append(converted, labels.Not(rm))
		}
	}

	return converted, nil
}
