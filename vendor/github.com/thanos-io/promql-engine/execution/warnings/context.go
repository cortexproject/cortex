package warnings

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/util/annotations"
)

type warningKey string

const key warningKey = "promql-warnings"

type warnings struct {
	mu    sync.Mutex
	warns annotations.Annotations
}

func newWarnings() *warnings {
	return &warnings{warns: annotations.Annotations{}}
}

func (w *warnings) add(warns annotations.Annotations) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warns = w.warns.Merge(warns)
}

func (w *warnings) get() annotations.Annotations {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.warns
}

func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, key, newWarnings())
}

func AddToContext(warns annotations.Annotations, ctx context.Context) {
	if len(warns) == 0 {
		return
	}
	w, ok := ctx.Value(key).(*warnings)
	if !ok {
		return
	}
	w.add(warns)
}

func FromContext(ctx context.Context) annotations.Annotations {
	return ctx.Value(key).(*warnings).get()
}
