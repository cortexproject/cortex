package util

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func BenchmarkMergeSlicesParallel(b *testing.B) {
	testCases := []struct {
		inputSize       int
		stringsPerInput int
		duplicateRatio  float64
	}{
		{
			inputSize:       100,
			stringsPerInput: 100,
			duplicateRatio:  0.3, // Deduped array size will be 70% of the total of the input
		},
		{
			inputSize:       100,
			stringsPerInput: 100,
			duplicateRatio:  0.8, // Deduped array size will be 20% of the total of the input
		},
		{
			inputSize:       100,
			stringsPerInput: 100,
			duplicateRatio:  0.95, // Deduped array size will be 5% of the total of the input
		},
		{
			inputSize:       150,
			stringsPerInput: 300,
			duplicateRatio:  0.3,
		},
		{
			inputSize:       150,
			stringsPerInput: 300,
			duplicateRatio:  0.8,
		},
		{
			inputSize:       150,
			stringsPerInput: 300,
			duplicateRatio:  0.95,
		},
	}

	randomStrings := GenerateRandomStrings()
	type ParallelismType int

	const (
		usingMap ParallelismType = iota
	)

	parallelism := []ParallelismType{usingMap, 1, 8}

	for _, tc := range testCases {
		input := make([][]string, tc.inputSize)
		unusedStrings := make([]string, min(len(randomStrings), tc.inputSize*tc.stringsPerInput))
		usedStrings := make([]string, 0, len(unusedStrings))
		copy(unusedStrings, randomStrings)

		for i := 0; i < tc.inputSize; i++ {
			stringsToBeReused := make([]string, len(usedStrings))
			copy(stringsToBeReused, usedStrings)
			for j := 0; j < tc.stringsPerInput; j++ {
				// Get a random string already used
				var s string
				if j < int(float64(tc.stringsPerInput)*tc.duplicateRatio) && i > 0 {
					index := rand.Int() % len(stringsToBeReused)
					s = stringsToBeReused[index]
					stringsToBeReused = append(stringsToBeReused[:index], stringsToBeReused[index+1:]...)
				} else {
					for {
						s = unusedStrings[0]
						usedStrings = append(usedStrings, s)
						unusedStrings = unusedStrings[1:]
						break
					}
				}
				input[i] = append(input[i], s)
			}
			sort.Strings(input[i])
		}
		for _, p := range parallelism {
			var name string
			if p == usingMap {
				name = fmt.Sprintf("usingMap,inputSize:%v,stringsPerInput:%v,duplicateRatio:%v", tc.inputSize, tc.stringsPerInput, tc.duplicateRatio)
			} else {
				name = fmt.Sprintf("parallelism:%v,inputSize:%v,stringsPerInput:%v,duplicateRatio:%v", p, tc.inputSize, tc.stringsPerInput, tc.duplicateRatio)
			}
			expected := sortUsingMap(input...)
			b.Run(name, func(b *testing.B) {
				// Run the benchmark.
				b.ReportAllocs()
				var r []string
				var err error
				for b.Loop() {
					if p == usingMap {
						r = sortUsingMap(input...)
						require.NotEmpty(b, r)
					} else {
						r, err = MergeSlicesParallel(context.Background(), int(p), input...)
						require.NoError(b, err)
						require.NotEmpty(b, r)
					}
				}
				require.Equal(b, r, expected)
			})
		}
	}
}

// TestMergeSlicesParallel_CanceledContextDoesNotLeak verifies that a canceled
// context makes MergeSlicesParallel return the context error without leaking the
// worker goroutines. When several batches observe the canceled context, each one
// tries to report its error, and before the fix every batch after the first
// blocked forever sending on the single-slot error channel (and so did the
// goroutine waiting on the WaitGroup).
func TestMergeSlicesParallel_CanceledContextDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Enough slices that the parallel path splits them into several batches, and
	// enough strings per slice that each batch iterates past
	// CheckContextEveryNIterations and notices the canceled context.
	const numSlices = 8
	const stringsPerSlice = 200

	input := make([][]string, numSlices)
	for i := range input {
		s := make([]string, stringsPerSlice)
		for j := range s {
			s[j] = fmt.Sprintf("%05d", j)
		}
		input[i] = s
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := MergeSlicesParallel(ctx, 8, input...)
	require.ErrorIs(t, err, context.Canceled)
}

func sortUsingMap(resps ...[]string) []string {
	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp {
			valueSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}

	sort.Strings(values)
	return values
}
