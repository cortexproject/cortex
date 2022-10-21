package querysharding

import (
	"encoding/base64"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	CortexShardByLabel = "__CORTEX_SHARD_BY__"
)

var (
	buffers = sync.Pool{New: func() interface{} {
		b := make([]byte, 0, 100)
		return &b
	}}
)

func InjectShardingInfo(query string, shardInfo *storepb.ShardInfo) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", err
	}
	b, err := shardInfo.Marshal()
	if err != nil {
		return "", err
	}
	eShardInfo := base64.StdEncoding.EncodeToString(b)
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		if selector, ok := n.(*parser.VectorSelector); ok {
			selector.LabelMatchers = append(selector.LabelMatchers, &labels.Matcher{
				Type:  labels.MatchEqual,
				Name:  CortexShardByLabel,
				Value: eShardInfo,
			})
		}
		return nil
	})

	return expr.String(), err
}

func ExtractShardingInfo(matchers []*labels.Matcher) ([]*labels.Matcher, *storepb.ShardInfo, error) {
	r := make([]*labels.Matcher, 0, len(matchers))

	shardInfo := storepb.ShardInfo{}
	for _, matcher := range matchers {
		if matcher.Name == CortexShardByLabel && matcher.Type == labels.MatchEqual {
			decoded, err := base64.StdEncoding.DecodeString(matcher.Value)
			if err != nil {
				return r, nil, err
			}
			err = shardInfo.Unmarshal(decoded)
			if err != nil {
				return r, nil, err
			}
		} else {
			r = append(r, matcher)
		}
	}

	return r, &shardInfo, nil
}

func ExtractShardingMatchers(matchers []*labels.Matcher) ([]*labels.Matcher, *storepb.ShardMatcher, error) {
	r, shardInfo, err := ExtractShardingInfo(matchers)

	if err != nil {
		return r, nil, err
	}

	return r, shardInfo.Matcher(&buffers), nil
}
