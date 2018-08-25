package batch

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

func TestStream(t *testing.T) {
	for i, tc := range []struct {
		input  []promchunk.Batch
		output batchStream
	}{
		{
			input:  []promchunk.Batch{mkBatch(0)},
			output: []promchunk.Batch{mkBatch(0)},
		},

		{
			input:  []promchunk.Batch{mkBatch(0), mkBatch(0)},
			output: []promchunk.Batch{mkBatch(0)},
		},

		{
			input:  []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize)},
			output: []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize)},
		},

		{
			input:  []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize), mkBatch(0), mkBatch(promchunk.BatchSize)},
			output: []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize)},
		},

		{
			input:  []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize / 2), mkBatch(promchunk.BatchSize)},
			output: []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize)},
		},

		{
			input:  []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize / 2), mkBatch(promchunk.BatchSize), mkBatch(3 * promchunk.BatchSize / 2), mkBatch(2 * promchunk.BatchSize)},
			output: []promchunk.Batch{mkBatch(0), mkBatch(promchunk.BatchSize), mkBatch(2 * promchunk.BatchSize)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			result := make(batchStream, len(tc.input))
			result = mergeBatches(tc.input, result)
			require.Equal(t, batchStream(tc.output), result)
		})
	}
}

func mkBatch(from int64) promchunk.Batch {
	var result promchunk.Batch
	for i := int64(0); i < promchunk.BatchSize; i++ {
		result.Timestamps[i] = from + i
		result.Values[i] = float64(from + i)
	}
	result.Length = promchunk.BatchSize
	return result
}
