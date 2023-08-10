package warnings

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/storage"
)

type warningKey string

const key warningKey = "promql-warnings"

type warnings struct {
	mu    sync.Mutex
	warns storage.Warnings
}

func newWarnings() *warnings {
	return &warnings{
		warns: make(storage.Warnings, 0),
	}
}

func (w *warnings) add(warns storage.Warnings) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warns = append(w.warns, warns...)
}

func (w *warnings) get() storage.Warnings {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.warns
}

func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, key, newWarnings())
}

func AddToContext(warns storage.Warnings, ctx context.Context) {
	if len(warns) == 0 {
		return
	}
	w, ok := ctx.Value(key).(*warnings)
	if !ok {
		return
	}
	w.add(warns)
}

func FromContext(ctx context.Context) storage.Warnings {
	return ctx.Value(key).(*warnings).get()
}
