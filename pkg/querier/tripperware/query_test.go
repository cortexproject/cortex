package tripperware

import (
	stdjson "encoding/json"
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestMarshalSampleStream(t *testing.T) {
	for i, tc := range []struct {
		sampleStream SampleStream
	}{
		{
			sampleStream: SampleStream{
				Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 1}},
			},
		},
		{
			sampleStream: SampleStream{
				Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"foo": "bar"})),
				Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 1}},
			},
		},
		{
			sampleStream: SampleStream{
				Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"foo": "bar", "test": "test", "a": "b"})),
				Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 1}, {Value: 2, TimestampMs: 2}, {Value: 3, TimestampMs: 3}},
			},
		},
	} {
		t.Run(fmt.Sprintf("test-case-%d", i), func(t *testing.T) {
			out1, err := json.Marshal(tc.sampleStream)
			require.NoError(t, err)
			out2, err := tc.sampleStream.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, out1, out2)
		})
	}
}

// MarshalJSON implements json.Marshaler.
func (s *SampleStream) MarshalJSON() ([]byte, error) {
	stream := struct {
		Metric model.Metric      `json:"metric"`
		Values []cortexpb.Sample `json:"values"`
	}{
		Metric: cortexpb.FromLabelAdaptersToMetric(s.Labels),
		Values: s.Samples,
	}
	return stdjson.Marshal(stream)
}
