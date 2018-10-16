package frontend

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStepAlign(t *testing.T) {
	for i, tc := range []struct {
		input, expected *QueryRangeRequest
	}{
		{
			input: &QueryRangeRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			expected: &QueryRangeRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},

		{
			input: &QueryRangeRequest{
				Start: 2,
				End:   102,
				Step:  10,
			},
			expected: &QueryRangeRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var result *QueryRangeRequest
			s := stepAlign{
				next: queryRangeHandlerFunc(func(_ context.Context, req *QueryRangeRequest) (*APIResponse, error) {
					result = req
					return nil, nil
				}),
			}
			s.Do(context.Background(), tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
