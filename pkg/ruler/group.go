package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/rules"
)

// group is a wrapper around a prometheus rules.Group, with a mutable appendable
type group struct {
	promGroup  *rules.Group
	appendable *appendableAppender
}

func newGroup(name string, rls []rules.Rule, appendable *appendableAppender, opts *rules.ManagerOptions) *group {
	delay := 0 * time.Second // Unused, so 0 value is fine.
	promGroup := rules.NewGroup(name, "none", delay, rls, opts)
	return &group{promGroup, appendable}
}

func (g *group) Eval(ctx context.Context, ts time.Time) {
	g.appendable.ctx = ctx
	g.promGroup.Eval(ctx, ts)
}

func (g *group) Rules() []rules.Rule {
	return g.promGroup.Rules()
}
