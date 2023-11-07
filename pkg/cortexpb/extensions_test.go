package cortexpb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestWriteRequest_Sign(t *testing.T) {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "user-1")

	tests := map[string]struct {
		w            *WriteRequest
		expectedSign string
	}{
		"small write with exemplar": {
			w:            createWriteRequest(10, true, "family1", "help1", "unit"),
			expectedSign: "v1/9125893422459502203",
		},
		"small write with exemplar and changed md": {
			w:            createWriteRequest(10, true, "family2", "help1", "unit"),
			expectedSign: "v1/18044786562323437562",
		},
		"small write without exemplar": {
			w:            createWriteRequest(10, false, "family1", "help1", "unit"),
			expectedSign: "v1/7697478040597284323",
		},
		"big write with exemplar": {
			w:            createWriteRequest(10000, true, "family1", "help1", "unit"),
			expectedSign: "v1/18402783317092766507",
		},
		"big write without exemplar": {
			w:            createWriteRequest(10000, false, "family1", "help1", "unit"),
			expectedSign: "v1/14973071954515615892",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// running multiple times in parallel to make sure no race
			itNumber := 1000
			wg := sync.WaitGroup{}
			wg.Add(itNumber)
			for i := 0; i < itNumber; i++ {
				go func() {
					defer wg.Done()
					s, err := tc.w.Sign(ctx)
					require.NoError(t, err)
					// Make sure this sign doesn't change
					require.Equal(t, tc.expectedSign, s)
				}()
			}
			wg.Wait()
		})
	}
}

func createWriteRequest(numTs int, exemplar bool, family string, help string, unit string) *WriteRequest {
	w := &WriteRequest{}
	w.Metadata = []*MetricMetadata{
		{
			MetricFamilyName: family,
			Help:             help,
			Unit:             unit,
		},
	}

	for i := 0; i < numTs; i++ {
		w.Timeseries = append(w.Timeseries, PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{
						Name:  fmt.Sprintf("Name-%v", i),
						Value: fmt.Sprintf("Value-%v", i),
					},
				},
			},
		})

		if exemplar {
			w.Timeseries[i].Exemplars = []Exemplar{
				{
					Labels: []LabelAdapter{
						{
							Name:  fmt.Sprintf("Ex-Name-%v", i),
							Value: fmt.Sprintf("Ex-Value-%v", i),
						},
					},
				},
			}
		}
	}

	return w
}
