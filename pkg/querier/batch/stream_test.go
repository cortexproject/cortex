package batch

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	for i, tc := range []struct {
		input  []batch
		output batchStream
	}{
		{
			input:  []batch{mkBatch(0)},
			output: []batch{mkBatch(0)},
		},

		{
			input:  []batch{mkBatch(0), mkBatch(0)},
			output: []batch{mkBatch(0)},
		},

		{
			input:  []batch{mkBatch(0), mkBatch(batchSize)},
			output: []batch{mkBatch(0), mkBatch(batchSize)},
		},

		{
			input:  []batch{mkBatch(0), mkBatch(batchSize), mkBatch(0), mkBatch(batchSize)},
			output: []batch{mkBatch(0), mkBatch(batchSize)},
		},

		{
			input:  []batch{mkBatch(0), mkBatch(batchSize / 2), mkBatch(batchSize)},
			output: []batch{mkBatch(0), mkBatch(batchSize)},
		},

		{
			input:  []batch{mkBatch(0), mkBatch(batchSize / 2), mkBatch(batchSize), mkBatch(3 * batchSize / 2), mkBatch(2 * batchSize)},
			output: []batch{mkBatch(0), mkBatch(batchSize), mkBatch(2 * batchSize)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			result := make(batchStream, len(tc.input))
			result = mergeBatches(tc.input, result)
			require.Equal(t, batchStream(tc.output), result)
		})
	}
}

func mkBatch(from int64) batch {
	var result batch
	for i := int64(0); i < batchSize; i++ {
		result.timestamps[i] = from + i
		result.values[i] = float64(from + i)
	}
	result.length = batchSize
	return result
}
