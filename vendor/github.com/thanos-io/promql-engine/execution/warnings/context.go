// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

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

func (w *warnings) add(warns error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warns = w.warns.Add(warns)
}

func (w *warnings) get() annotations.Annotations {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.warns
}

func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, key, newWarnings())
}

func AddToContext(warn error, ctx context.Context) {
	w, ok := ctx.Value(key).(*warnings)
	if !ok {
		return
	}
	w.add(warn)
}

func FromContext(ctx context.Context) annotations.Annotations {
	return ctx.Value(key).(*warnings).get()
}
