package client

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestMarshall is useful to try out various optimisation on the unmarshalling code.
func TestMarshall(t *testing.T) {
	const numSeries = 10
	recorder := httptest.NewRecorder()
	{
		req := WriteRequest{}
		for i := 0; i < numSeries; i++ {
			req.Timeseries = append(req.Timeseries, PreallocTimeseries{
				&TimeSeries{
					Labels: []LabelAdapter{
						{"foo", strconv.Itoa(i)},
					},
					Samples: []Sample{
						{TimestampMs: int64(i), Value: float64(i)},
					},
				},
			})
		}
		err := util.SerializeProtoResponse(recorder, &req, util.RawSnappy)
		require.NoError(t, err)
	}

	{
		const (
			tooSmallSize = 1
			plentySize   = 1024 * 1024
		)
		req := WriteRequest{}
		_, err := util.ParseProtoReader(context.Background(), recorder.Body, recorder.Body.Len(), tooSmallSize, &req, util.RawSnappy)
		require.Error(t, err)
		_, err = util.ParseProtoReader(context.Background(), recorder.Body, recorder.Body.Len(), plentySize, &req, util.RawSnappy)
		require.NoError(t, err)
		require.Equal(t, numSeries, len(req.Timeseries))
	}
}
