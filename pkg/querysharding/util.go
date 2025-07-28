package querysharding

import (
	"encoding/base64"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	cortexparser "github.com/cortexproject/cortex/pkg/parser"
)

const (
	CortexShardByLabel = "__CORTEX_SHARD_BY__"
)

var (
	buffers = sync.Pool{New: func() interface{} {
		b := make([]byte, 0, 100)
		return &b
	}}

	stop = errors.New("stop")
)

func InjectShardingInfo(query string, shardInfo *storepb.ShardInfo) (string, error) {
	expr, err := cortexparser.ParseExpr(query)
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

type disableBinaryExpressionAnalyzer struct {
	analyzer querysharding.Analyzer
}

// NewDisableBinaryExpressionAnalyzer is a wrapper around the analyzer that disables binary expressions.
func NewDisableBinaryExpressionAnalyzer(analyzer querysharding.Analyzer) *disableBinaryExpressionAnalyzer {
	return &disableBinaryExpressionAnalyzer{analyzer: analyzer}
}

func (d *disableBinaryExpressionAnalyzer) Analyze(query string) (querysharding.QueryAnalysis, error) {
	analysis, err := d.analyzer.Analyze(query)
	if err != nil || !analysis.IsShardable() {
		return analysis, err
	}

	expr, _ := cortexparser.ParseExpr(query)
	isShardable := true
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		switch n := node.(type) {
		case *parser.BinaryExpr:
			// No vector matching means one operand is not vector. Skip it.
			if n.VectorMatching == nil {
				return nil
			}
			// Vector matching ignore will add MetricNameLabel as sharding label.
			// Mark this type of query not shardable.
			if !n.VectorMatching.On {
				isShardable = false
				return stop
			}
		}
		return nil
	})
	if !isShardable {
		// Mark as not shardable.
		return querysharding.QueryAnalysis{}, nil
	}
	return analysis, nil
}
