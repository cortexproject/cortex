// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"maps"
	"slices"

	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

type ProjectionOptimizer struct {
	SeriesHashLabel string
}

func (p ProjectionOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	p.pushProjection(&plan, nil)
	return plan, nil
}

// pushProjection recursively traverses the tree and pushes projection information down.
func (p ProjectionOptimizer) pushProjection(node *Node, projection *Projection) {
	switch n := (*node).(type) {
	case *VectorSelector:
		if projection != nil {
			n.Projection = projection
		} else {
			// Set dummy projection.
			n.Projection = &Projection{}
		}

	case *Aggregation:
		// Special handling for aggregation functions that need all labels
		// regardless of grouping (topk, bottomk, limitk, limit_ratio)
		switch n.Op {
		case parser.TOPK, parser.BOTTOMK, parser.LIMITK, parser.LIMIT_RATIO:
			// These functions need all labels, so clear any requirements
			p.pushProjection(&n.Expr, nil)
			return
		}

		// For aggregations, we directly use the grouping labels
		grouping := n.Grouping
		groupingProjection := &Projection{
			Labels:  grouping,
			Include: !n.Without,
		}
		// Note that we don't push projection to Aggregation.Param as they are not
		// selecting data for the aggregation.
		p.pushProjection(&n.Expr, groupingProjection)

		if p.SeriesHashLabel != "" && n.Without {
			n.Grouping = append(grouping, p.SeriesHashLabel)
		}

	case *Binary:
		var highCard, lowCard = n.LHS, n.RHS

		if n.VectorMatching == nil || (!n.VectorMatching.On && len(n.VectorMatching.MatchingLabels) == 0) {
			if IsConstantExpr(lowCard) {
				p.pushProjection(&highCard, projection)
			} else {
				p.pushProjection(&highCard, nil)
			}

			if IsConstantExpr(highCard) {
				p.pushProjection(&lowCard, projection)
			} else {
				p.pushProjection(&lowCard, nil)
			}
			return
		}

		if n.VectorMatching.Card == parser.CardOneToOne {
			proj := &Projection{
				Labels:  n.VectorMatching.MatchingLabels,
				Include: n.VectorMatching.On,
			}

			for _, child := range n.Children() {
				p.pushProjection(child, proj)
			}

			if !n.VectorMatching.On && p.SeriesHashLabel != "" {
				n.VectorMatching.MatchingLabels = append(n.VectorMatching.MatchingLabels, p.SeriesHashLabel)
			}
			return
		}

		if n.VectorMatching.Card == parser.CardOneToMany {
			highCard, lowCard = lowCard, highCard
		}

		// Handle high card side projection. Only ignoring mode is supported.
		hcProjection := &Projection{}
		// Only push projection for high card side if there is an outer projection available
		// to remove series hash
		if projection != nil && projection.Include {
			// Include labels are from low card side so we don't need to fetch
			// them from high card side if include labels are not used as join keys.
			hcProjection.Labels = n.VectorMatching.Include
			if !n.VectorMatching.On {
				hcProjection.Labels = intersect(hcProjection.Labels, n.VectorMatching.MatchingLabels)
			}
		}
		if len(hcProjection.Labels) > 1 {
			p.pushProjection(&highCard, hcProjection)
		} else {
			// If there is only 1 label to project then it is not worth to push projection
			// down to high card side as calculating hash might be more expensive.
			p.pushProjection(&highCard, nil)
		}

		// Handle low card side projection.
		lcProjection := extendProjection(Projection{
			Include: n.VectorMatching.On,
			Labels:  n.VectorMatching.MatchingLabels,
		}, n.VectorMatching.Include)
		p.pushProjection(&lowCard, &lcProjection)

		if !n.VectorMatching.On && p.SeriesHashLabel != "" {
			n.VectorMatching.MatchingLabels = append(n.VectorMatching.MatchingLabels, p.SeriesHashLabel)
		}
		return

	case *FunctionCall:
		// Handle function-specific label requirements.
		updatedProjection := getFunctionLabelRequirements(n.Func.Name, n.Args, projection)
		for _, child := range n.Children() {
			p.pushProjection(child, updatedProjection)
		}

	default:
		// For other node types, propagate to children
		for _, child := range (*node).Children() {
			p.pushProjection(child, projection)
		}
	}
}

func extendProjection(projection Projection, lbls []string) Projection {
	var extendedLabels []string
	if projection.Include {
		extendedLabels = union(projection.Labels, lbls)
	} else {
		extendedLabels = subtract(projection.Labels, lbls)
	}
	return Projection{
		Include: projection.Include,
		Labels:  extendedLabels,
	}
}

// unwrapStepInvariantExpr recursively unwraps step invariant expressions to get to the underlying node.
func unwrapStepInvariantExpr(node Node) Node {
	if stepInvariant, ok := node.(*StepInvariantExpr); ok {
		return unwrapStepInvariantExpr(stepInvariant.Expr)
	}
	return node
}

// getFunctionLabelRequirements returns an updated projection based on function-specific requirements.
func getFunctionLabelRequirements(funcName string, args []Node, projection *Projection) *Projection {
	if projection == nil {
		projection = &Projection{}
	}
	result := &Projection{
		Labels:  make([]string, len(projection.Labels)),
		Include: projection.Include,
	}
	copy(result.Labels, projection.Labels)

	// Add function-specific required labels
	switch funcName {
	case "absent_over_time", "absent", "scalar":
		return &Projection{
			Labels:  []string{},
			Include: true,
		}
	case "histogram_quantile":
		// Unsafe to push projection down for histogram_quantile as it requires le label.
		return nil
	case "label_replace":
		dstArg := unwrapStepInvariantExpr(args[1])
		if dstLit, ok := dstArg.(*StringLiteral); ok {
			dstLabel := dstLit.Val
			needed := slices.Contains(result.Labels, dstLabel)
			needSourceLabels := (result.Include && needed) || (!result.Include && !needed)
			if !needSourceLabels {
				return result
			}

			srcArg := unwrapStepInvariantExpr(args[3])
			if strLit, ok := srcArg.(*StringLiteral); ok {
				if result.Include && needed {
					result.Labels = append(result.Labels, strLit.Val)
				} else {
					result.Labels = slices.DeleteFunc(result.Labels, func(s string) bool {
						return s == strLit.Val
					})
				}
			}
		}
	case "label_join":
		dstArg := unwrapStepInvariantExpr(args[1])
		if dstLit, ok := dstArg.(*StringLiteral); ok {
			dstLabel := dstLit.Val
			needed := slices.Contains(result.Labels, dstLabel)
			needSourceLabels := (result.Include && needed) || (!result.Include && !needed)
			if !needSourceLabels {
				return result
			}

			// Only if the destination label is needed, we need the source labels
			for i := 3; i < len(args); i++ {
				srcArg := unwrapStepInvariantExpr(args[i])
				if strLit, ok := srcArg.(*StringLiteral); ok {
					if result.Include && needed {
						result.Labels = append(result.Labels, strLit.Val)
					} else {
						result.Labels = slices.DeleteFunc(result.Labels, func(s string) bool {
							return s == strLit.Val
						})
					}
				}
			}
		}
	}

	return result
}

// union returns the union of two string slices.
func union(l1 []string, l2 []string) []string {
	m := make(map[string]struct{})
	for _, s := range l1 {
		m[s] = struct{}{}
	}
	for _, s := range l2 {
		m[s] = struct{}{}
	}
	return slices.Collect(maps.Keys(m))
}

// subtract returns the intersection of two string slices.
func subtract(l1 []string, l2 []string) []string {
	m := make(map[string]struct{})
	for _, s := range l1 {
		m[s] = struct{}{}
	}
	for _, s := range l2 {
		delete(m, s)
	}
	return slices.Collect(maps.Keys(m))
}

func intersect(l1 []string, l2 []string) []string {
	m := make(map[string]struct{})
	for _, s := range l1 {
		m[s] = struct{}{}
	}
	var result []string
	for _, s := range l2 {
		if _, ok := m[s]; ok {
			result = append(result, s)
		}
	}
	return result
}
