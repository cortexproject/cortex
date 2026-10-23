package storegateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func newTestSeries(value string) *storepb.Series {
	return &storepb.Series{Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("series", value))}
}

func TestChannelSeriesServer_CollectsSeriesWarningsAndHints(t *testing.T) {
	s := newChannelSeriesServer(context.Background(), 10, true)

	anyHints, err := types.MarshalAny(&hintspb.SeriesResponseHints{
		QueriedBlocks: []hintspb.Block{{Id: "block-1"}},
		QueryStats:    &hintspb.QueryStats{BlocksQueried: 1},
	})
	require.NoError(t, err)

	require.NoError(t, s.Send(storepb.NewSeriesResponse(newTestSeries("a"))))
	require.NoError(t, s.Send(storepb.NewBatchResponse([]*storepb.Series{newTestSeries("b"), newTestSeries("c")})))
	require.NoError(t, s.Send(storepb.NewWarnSeriesResponse(errors.New("warning-1"))))
	require.NoError(t, s.Send(storepb.NewHintsSeriesResponse(anyHints)))
	s.Close(nil)

	var got []string
	for s.Next() {
		lset, _ := s.At()
		got = append(got, lset.Get("series"))
	}
	require.NoError(t, s.Err())
	assert.Equal(t, []string{"a", "b", "c"}, got)

	assert.Len(t, s.warnings, 1)
	assert.Contains(t, s.warnings, "warning-1")
	assert.Equal(t, []hintspb.Block{{Id: "block-1"}}, s.hints.QueriedBlocks)
	require.NotNil(t, s.hints.QueryStats)
	assert.Equal(t, int64(1), s.hints.QueryStats.BlocksQueried)
}

func TestChannelSeriesServer_CloseWithError(t *testing.T) {
	s := newChannelSeriesServer(context.Background(), 10, true)
	require.NoError(t, s.Send(storepb.NewSeriesResponse(newTestSeries("a"))))

	producerErr := errors.New("producer failed")
	s.Close(producerErr)

	// Series pushed before the failure are still drained, then the error is exposed.
	require.True(t, s.Next())
	require.False(t, s.Next())
	assert.ErrorIs(t, s.Err(), producerErr)
}

func TestChannelSeriesServer_SendUnblocksOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newChannelSeriesServer(ctx, 1, true)

	// The first series fills the buffer; with no consumer the next one would block forever.
	require.NoError(t, s.Send(storepb.NewSeriesResponse(newTestSeries("a"))))

	cancel()
	err := s.Send(storepb.NewSeriesResponse(newTestSeries("b")))
	assert.ErrorIs(t, err, context.Canceled)
}

func TestChannelSeriesServer_CopyOnSend(t *testing.T) {
	for _, copyOnSend := range []bool{true, false} {
		t.Run(fmt.Sprintf("copyOnSend=%t", copyOnSend), func(t *testing.T) {
			s := newChannelSeriesServer(context.Background(), 1, copyOnSend)
			sent := newTestSeries("a")
			require.NoError(t, s.Send(storepb.NewSeriesResponse(sent)))
			s.Close(nil)

			require.True(t, s.Next())
			lset, _ := s.At()
			assert.Equal(t, "a", lset.Get("series"))
			if copyOnSend {
				// A copied series must not alias the sender's memory, which may be recycled.
				assert.NotSame(t, sent, s.cur)
			} else {
				assert.Same(t, sent, s.cur)
			}
		})
	}
}
