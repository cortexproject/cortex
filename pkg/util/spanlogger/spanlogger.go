package spanlogger

import (
	"context"

	"github.com/go-kit/kit/log"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/cortexproject/cortex/pkg/util"
)

// SpanLogger unifies tracing and logging, to reduce repetition.
type SpanLogger struct {
	log.Logger
	opentracing.Span
}

// New makes a new SpanLogger.
func New(ctx context.Context, method string, kvps ...interface{}) (*SpanLogger, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, method)
	logger := &SpanLogger{
		Logger: log.With(util.WithContext(ctx, util.Logger), "method", method),
		Span:   span,
	}
	if len(kvps) > 0 {
		logger.Log(kvps...)
	}
	return logger, ctx
}

func (s *SpanLogger) Log(kvps ...interface{}) error {
	s.Logger.Log(kvps...)
	fields, err := otlog.InterleavedKVToFields(kvps...)
	if err != nil {
		return err
	}
	s.Span.LogFields(fields...)
	return nil
}
