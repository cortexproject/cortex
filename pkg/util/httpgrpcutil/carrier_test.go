package httpgrpcutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
)

func TestHttpgrpcHeadersCarrier_Set(t *testing.T) {
	carrier := &HttpgrpcHeadersCarrier{}

	carrier.Set("X-Trace-ID", "abc123")
	carrier.Set("X-Span-ID", "def456")

	require.Len(t, carrier.Headers, 2)
	assert.Equal(t, "X-Trace-ID", carrier.Headers[0].Key)
	assert.Equal(t, []string{"abc123"}, carrier.Headers[0].Values)
	assert.Equal(t, "X-Span-ID", carrier.Headers[1].Key)
	assert.Equal(t, []string{"def456"}, carrier.Headers[1].Values)
}

func TestHttpgrpcHeadersCarrier_ForeachKey(t *testing.T) {
	carrier := &HttpgrpcHeadersCarrier{
		Headers: []*httpgrpc.Header{
			{Key: "key1", Values: []string{"val1a", "val1b"}},
			{Key: "key2", Values: []string{"val2"}},
		},
	}

	var collected []string
	err := carrier.ForeachKey(func(key, val string) error {
		collected = append(collected, key+"="+val)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"key1=val1a", "key1=val1b", "key2=val2"}, collected)
}

func TestHttpgrpcHeadersCarrier_ForeachKey_returnsHandlerError(t *testing.T) {
	carrier := &HttpgrpcHeadersCarrier{
		Headers: []*httpgrpc.Header{
			{Key: "key1", Values: []string{"val1"}},
		},
	}

	handlerErr := errors.New("handler failed")
	err := carrier.ForeachKey(func(key, val string) error {
		return handlerErr
	})

	assert.Equal(t, handlerErr, err)
}

func TestHttpgrpcHeadersCarrier_ForeachKey_emptyHeaders(t *testing.T) {
	carrier := &HttpgrpcHeadersCarrier{}

	err := carrier.ForeachKey(func(key, val string) error {
		t.Fatal("handler should not be called for empty headers")
		return nil
	})

	assert.NoError(t, err)
}
