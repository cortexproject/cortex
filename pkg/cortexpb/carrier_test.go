package cortexpb

import (
	"testing"

	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamWriteRequestCarrier_RoundTrip(t *testing.T) {
	tracer := mocktracer.New()
	span := tracer.StartSpan("Distributor.Push")
	defer span.Finish()

	req := &StreamWriteRequest{TenantID: "user-1"}
	require.NoError(t, InjectSpanIntoStreamWriteRequest(tracer, span, req))
	require.NotEmpty(t, req.TraceContext, "the trace context must be carried by the message itself")

	extracted, err := GetParentSpanForStreamWriteRequest(tracer, req)
	require.NoError(t, err)
	require.NotNil(t, extracted)

	assert.Equal(t, span.Context().(mocktracer.MockSpanContext).TraceID, extracted.(mocktracer.MockSpanContext).TraceID)
	assert.Equal(t, span.Context().(mocktracer.MockSpanContext).SpanID, extracted.(mocktracer.MockSpanContext).SpanID)
}
