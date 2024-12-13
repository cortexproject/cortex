package ruler

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestDecode(t *testing.T) {
	jsonDecoder := JsonDecoder{}

	tests := []struct {
		description     string
		body            string
		expectedVector  promql.Vector
		expectedWarning Warning
		expectedErr     error
	}{
		{
			description: "empty vector",
			body: `{
					"status": "success",
					"data": {"resultType":"vector","result":[]}
				}`,
			expectedVector:  promql.Vector{},
			expectedErr:     nil,
			expectedWarning: nil,
		},
		{
			description: "vector with series",
			body: `{
					"status": "success",
					"data": {
						"resultType": "vector",
						"result": [
							{
								"metric": {"foo":"bar"},
								"value": [1724146338.123,"1.234"]
							},
							{
								"metric": {"bar":"baz"},
								"value": [1724146338.456,"5.678"]
							}
						]
					}
				}`,
			expectedVector: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1724146338123,
					F:      1.234,
				},
				{
					Metric: labels.FromStrings("bar", "baz"),
					T:      1724146338456,
					F:      5.678,
				},
			},
			expectedErr:     nil,
			expectedWarning: nil,
		},
		{
			description: "scalar",
			body: `{
					"status": "success",
					"data": {"resultType":"scalar","result":[1724146338.123,"1.234"]}
				}`,
			expectedVector: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1724146338123,
					F:      1.234,
				},
			},
			expectedErr:     nil,
			expectedWarning: nil,
		},
		{
			description: "matrix",
			body: `{
					"status": "success",
					"data": {"resultType":"matrix","result":[]}
				}`,
			expectedVector:  nil,
			expectedErr:     errors.New("rule result is not a vector or scalar"),
			expectedWarning: nil,
		},
		{
			description: "string",
			body: `{
					"status": "success",
					"data": {"resultType":"string","result":[1724146338.123,"string"]}
				}`,
			expectedVector:  nil,
			expectedErr:     errors.New("rule result is not a vector or scalar"),
			expectedWarning: nil,
		},
		{
			description: "error with warnings",
			body: `{
					"status": "error",
					"errorType": "errorExec",
					"error": "something wrong",
					"warnings": ["a","b","c"]
				}`,
			expectedVector:  nil,
			expectedErr:     errors.New("failed to execute query with error: something wrong"),
			expectedWarning: []string{"a", "b", "c"},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			vector, warning, err := jsonDecoder.Decode([]byte(test.body))
			require.Equal(t, test.expectedVector, vector)
			require.Equal(t, test.expectedWarning, warning)
			require.Equal(t, test.expectedErr, err)
		})
	}
}
