package builder

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeLabels(t *testing.T) {
	for name, tc := range map[string]struct {
		input labels.Labels

		expectedOutput labels.Labels
		expectedError  error
	}{
		"good labels": {
			input:          fromStrings("__name__", "hello", "label1", "world"),
			expectedOutput: fromStrings("__name__", "hello", "label1", "world"),
			expectedError:  nil,
		},
		"not sorted": {
			input:          fromStrings("label1", "world", "__name__", "hello"),
			expectedOutput: fromStrings("__name__", "hello", "label1", "world"),
			expectedError:  nil,
		},
		"duplicate with same value": {
			input:          fromStrings("__name__", "hello", "label1", "world", "label1", "world"),
			expectedOutput: fromStrings("__name__", "hello", "label1", "world"),
			expectedError:  nil,
		},
		"not sorted, duplicate with same value": {
			input:          fromStrings("label1", "world", "__name__", "hello", "label1", "world"),
			expectedOutput: fromStrings("__name__", "hello", "label1", "world"),
			expectedError:  nil,
		},
		"duplicate with different value": {
			input:          fromStrings("label1", "world1", "__name__", "hello", "label1", "world2"),
			expectedOutput: fromStrings("__name__", "hello", "label1", "world1", "label1", "world2"), // sorted
			expectedError:  errDuplicateLabelsDifferentValue,
		},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := normalizeLabels(tc.input)

			assert.Equal(t, tc.expectedOutput, out)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

// Similar to labels.FromStrings, but doesn't do sorting.
func fromStrings(ss ...string) labels.Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	var res labels.Labels
	for i := 0; i < len(ss); i += 2 {
		res = append(res, labels.Label{Name: ss[i], Value: ss[i+1]})
	}
	return res
}
