package queryrange

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStepAlign(t *testing.T) {
	for i, tc := range []struct {
		input, expected *Request
	}{
		{
			input: &Request{
				Start: 0,
				End:   100,
				Step:  10,
			},
			expected: &Request{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},

		{
			input: &Request{
				Start: 2,
				End:   102,
				Step:  10,
			},
			expected: &Request{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var result *Request
			s := stepAlign{
				next: HandlerFunc(func(_ context.Context, req *Request) (*APIResponse, error) {
					result = req
					return nil, nil
				}),
			}
			s.Do(context.Background(), tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
