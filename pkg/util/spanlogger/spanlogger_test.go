package spanlogger

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestSpanLogger_Log(t *testing.T) {
	span, ctx := New(context.Background(), "test", "bar")
	_ = span.Log("foo")
	newSpan := FromContext(ctx)
	require.Equal(t, span.Span, newSpan.Span)
	_ = newSpan.Log("bar")
	noSpan := FromContext(context.Background())
	_ = noSpan.Log("foo")
	require.Error(t, noSpan.Error(errors.New("err")))
	require.NoError(t, noSpan.Error(nil))
}
