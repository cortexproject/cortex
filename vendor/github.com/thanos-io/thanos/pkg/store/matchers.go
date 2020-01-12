package store

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func translateMatcher(m storepb.LabelMatcher) (*labels.Matcher, error) {
	switch m.Type {
	case storepb.LabelMatcher_EQ:
		return labels.NewMatcher(labels.MatchEqual, m.Name, m.Value)

	case storepb.LabelMatcher_NEQ:
		return labels.NewMatcher(labels.MatchNotEqual, m.Name, m.Value)

	case storepb.LabelMatcher_RE:
		return labels.NewMatcher(labels.MatchRegexp, m.Name, m.Value)

	case storepb.LabelMatcher_NRE:
		return labels.NewMatcher(labels.MatchNotRegexp, m.Name, m.Value)
	}
	return nil, errors.Errorf("unknown label matcher type %d", m.Type)
}

func translateMatchers(ms []storepb.LabelMatcher) (res []*labels.Matcher, err error) {
	for _, m := range ms {
		r, err := translateMatcher(m)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}
