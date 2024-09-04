package queryrange

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestStepAlign(t *testing.T) {
	for i, tc := range []struct {
		input, expected *tripperware.PrometheusRequest
	}{
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			expected: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},

		{
			input: &tripperware.PrometheusRequest{
				Start: 2,
				End:   102,
				Step:  10,
			},
			expected: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			var result *tripperware.PrometheusRequest
			s := stepAlign{
				next: tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					result = req.(*tripperware.PrometheusRequest)
					return nil, nil
				}),
			}
			_, err := s.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}
