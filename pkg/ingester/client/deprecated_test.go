package client

import (
	"encoding/json"
	stdlibjson "encoding/json"
	"math"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// These tests verifies that jsoninter and stdlib json uses our custom method for marshalling.
// This is a copy of tests in cortexpb package, but using deprecated client.Sample type.
func TestJsoniterMarshalForSample(t *testing.T) {
	testMarshalling(t, jsoniter.Marshal, "test sample")
}

func TestStdlibJsonMarshalForSample(t *testing.T) {
	testMarshalling(t, json.Marshal, "json: error calling MarshalJSON for type cortexpb.Sample: test sample")
}

func testMarshalling(t *testing.T, marshalFn func(v interface{}) ([]byte, error), expectedError string) {
	cortexpb.IsTesting = true
	defer func() { cortexpb.IsTesting = false }()

	out, err := marshalFn(Sample{Value: 12345, TimestampMs: 98765})
	require.NoError(t, err)
	require.Equal(t, `[98.765,"12345"]`, string(out))

	_, err = marshalFn(Sample{Value: math.NaN(), TimestampMs: 0})
	require.EqualError(t, err, expectedError)

	// If not testing, we get normal output.
	cortexpb.IsTesting = false
	out, err = marshalFn(Sample{Value: math.NaN(), TimestampMs: 0})
	require.NoError(t, err)
	require.Equal(t, `[0,"NaN"]`, string(out))
}

// These tests verifies that jsoninter and stdlib json uses our custom method for unmarshalling.
// This is a copy of tests in cortexpb package, but using deprecated client.Sample type.
func TestJsoniterUnmarshalForSample(t *testing.T) {
	testUnmarshalling(t, jsoniter.Unmarshal, "test sample")
}

func TestStdlibJsonUnmarshalForSample(t *testing.T) {
	testUnmarshalling(t, stdlibjson.Unmarshal, "test sample")
}

func testUnmarshalling(t *testing.T, unmarshalFn func(data []byte, v interface{}) error, expectedError string) {
	cortexpb.IsTesting = true
	defer func() { cortexpb.IsTesting = false }()

	sample := Sample{}

	err := unmarshalFn([]byte(`[98.765,"12345"]`), &sample)
	require.NoError(t, err)
	require.Equal(t, Sample{Value: 12345, TimestampMs: 98765}, sample)

	err = unmarshalFn([]byte(`[0.0,"NaN"]`), &sample)
	require.EqualError(t, err, expectedError)

	cortexpb.IsTesting = false
	err = unmarshalFn([]byte(`[0.0,"NaN"]`), &sample)
	require.NoError(t, err)
	require.Equal(t, int64(0), sample.TimestampMs)
	require.True(t, math.IsNaN(sample.Value))
}
