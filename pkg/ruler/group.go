package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/rules"
)

// wrappedGroup is a wrapper around a prometheus rules.Group, with a mutable appendable
// appendable stored here will be the same appendable as in promGroup.opts.Appendable
type wrappedGroup struct {
	promGroup  *rules.Group
	appendable *appendableAppender
}

func newGroup(name string, rls []rules.Rule, appendable *appendableAppender, opts *rules.ManagerOptions) *wrappedGroup {
	delay := 0 * time.Second // Unused, so 0 value is fine.
	promGroup := rules.NewGroup(name, "none", delay, rls, false, opts)
	return &wrappedGroup{promGroup, appendable}
}

func (g *wrappedGroup) Eval(ctx context.Context, ts time.Time) {
	g.appendable.ctx = ctx
	g.promGroup.Eval(ctx, ts)
}

func (g *wrappedGroup) Rules() []rules.Rule {
	return g.promGroup.Rules()
}
