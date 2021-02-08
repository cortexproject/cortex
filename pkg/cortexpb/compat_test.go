package cortexpb

import (
	"math"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

// This test verifies that jsoninter uses our custom method for marshalling.
// We do that by using "test sample" recognized by marshal function when in testing mode.
func TestJsoniterMarshal(t *testing.T) {
	isTesting = true
	defer func() { isTesting = false }()

	out, err := jsoniter.Marshal(Sample{Value: 12345, TimestampMs: 98765})
	require.NoError(t, err)
	require.Equal(t, `[98.765,"12345"]`, string(out))

	_, err = json.Marshal(Sample{Value: math.NaN(), TimestampMs: math.MinInt64})
	require.EqualError(t, err, "test sample")

	// If not testing, we get normal output.
	isTesting = false
	out, err = json.Marshal(Sample{Value: math.NaN(), TimestampMs: math.MinInt64})
	require.NoError(t, err)
	require.Equal(t, `[-9223372036854776,"NaN"]`, string(out))
}

// This test verifies that jsoninter uses our custom method for unmarshalling Sample.
// As with Marshal, we rely on testing mode and special value that reports error.
func TestJsoniterUnmarshal(t *testing.T) {
	isTesting = true
	defer func() { isTesting = false }()

	sample := Sample{}

	err := jsoniter.UnmarshalFromString(`[98.765,"12345"]`, &sample)
	require.NoError(t, err)
	require.Equal(t, Sample{Value: 12345, TimestampMs: 98765}, sample)

	err = jsoniter.UnmarshalFromString(`[0.0,"NaN"]`, &sample)
	require.EqualError(t, err, "test sample")

	isTesting = false
	err = jsoniter.UnmarshalFromString(`[0.0,"NaN"]`, &sample)
	require.NoError(t, err)
	require.Equal(t, int64(0), sample.TimestampMs)
	require.True(t, math.IsNaN(sample.Value))
}
