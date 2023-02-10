package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func TestNewResource(t *testing.T) {
	ctx := context.Background()
	target := "ingester"
	r, err := newResource(ctx, target, nil)
	require.NoError(t, err)

	set := r.Set()
	name, ok := set.Value(semconv.ServiceNameKey)
	require.True(t, ok)
	require.Equal(t, name.AsString(), target)
}
