package frontend

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStepAlign(t *testing.T) {
	for i, tc := range []struct {
		input, expected *queryRangeRequest
	}{
		{
			input: &queryRangeRequest{
				start: 0,
				end:   100,
				step:  10,
			},
			expected: &queryRangeRequest{
				start: 0,
				end:   100,
				step:  10,
			},
		},

		{
			input: &queryRangeRequest{
				start: 2,
				end:   102,
				step:  10,
			},
			expected: &queryRangeRequest{
				start: 0,
				end:   100,
				step:  10,
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var result *queryRangeRequest
			s := stepAlign{
				next: queryRangeHandlerFunc(func(_ context.Context, req *queryRangeRequest) (*apiResponse, error) {
					result = req
					return nil, nil
				}),
			}
			s.Do(context.Background(), tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
