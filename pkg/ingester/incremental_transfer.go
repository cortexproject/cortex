package ingester

import (
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

var (
	blockedRanges = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_blocked_ranges",
		Help: "The current number of ranges that will not accept writes by this ingester.",
	})
)

func init() {
	prometheus.MustRegister(blockedRanges)
}

// TransferChunksSubset accepts chunks from a client and moves them into the local Ingester.
func (i *Ingester) TransferChunksSubset(stream client.Ingester_TransferChunksSubsetServer) error {
	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	fromIngesterID, seriesReceived, err := i.acceptChunksFromStream(acceptChunksOptions{
		States: i.userStates,
		Stream: stream,
	})
	if err != nil {
		return err
	}

	if err := stream.SendAndClose(&client.TransferChunksResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing SendChunks stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}

	// Target ingester may not have had any streams to send.
	if seriesReceived == 0 {
		return nil
	} else if fromIngesterID == "" {
		level.Error(util.Logger).Log("msg", "received TransferChunks request with no ID from ingester")
		return fmt.Errorf("no ingester id")
	}

	return nil
}

// GetChunksSubset accepts a get request from a client and sends all chunks from the serving ingester
// that fall within the given range to the client.
func (i *Ingester) GetChunksSubset(req *client.GetChunksRequest, stream client.Ingester_GetChunksSubsetServer) error {
	userStatesCopy := i.userStates.cp()
	if len(userStatesCopy) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	// Block the ranges locally so we don't receive traffic for it anymore. The caller should decide
	// when ranges should be unblocked: we should continue to reject writes for as long as we may
	// receive them. When the joining token has been completely inserted into the ring, it will
	// be safe to remove the blocks.
	i.BlockRanges(req.Ranges)

	return i.pushChunksToStream(pushChunksOptions{
		States:        userStatesCopy,
		Stream:        stream,
		MarkAsFlushed: req.Move,

		IncludeFunc: func(pair fingerprintSeriesPair) bool {
			return inAnyRange(pair.series.token, req.Ranges)
		},
	})
}

// BlockRanges configures a range of token values to be blocked. When a range
// is blocked, the Ingester will no longer accept pushes for any streams whose
// token falls within the blocked ranges. Unblocking the range re-enables those
// pushes.
//
// Blocked ranges will automatically be unblocked after the RangeBlockPeriod
// configuration variable. This acts as a failsafe to prevent blocked ranges
// sitting around forever if a joining ingester crashes, as writes will continue
// to go to us and get rejected for as long as the blocked range exists.
func (i *Ingester) BlockRanges(ranges []ring.TokenRange) {
	i.blockedTokenMtx.Lock()
	defer i.blockedTokenMtx.Unlock()

	for _, rg := range ranges {
		if exist := i.blockedRanges[rg]; exist {
			continue
		}
		i.blockedRanges[rg] = true
		blockedRanges.Inc()
	}

	go func() {
		<-time.After(i.cfg.RangeBlockPeriod)
		i.UnblockRanges(context.Background(), &client.UnblockRangesRequest{Ranges: ranges})
	}()
}

// UnblockRanges manually removes blocks for the provided ranges.
func (i *Ingester) UnblockRanges(ctx context.Context, in *client.UnblockRangesRequest) (*client.UnblockRangesResponse, error) {
	i.blockedTokenMtx.Lock()
	defer i.blockedTokenMtx.Unlock()

	for _, rg := range in.Ranges {
		if exist := i.blockedRanges[rg]; !exist {
			level.Debug(util.Logger).Log("msg", "token range not blocked", "from", rg.From, "to", rg.To)
			continue
		}

		delete(i.blockedRanges, rg)
		blockedRanges.Dec()
	}

	return &client.UnblockRangesResponse{}, nil
}

// SendChunkRanges connects to the ingester at targetAddr and sends all chunks
// for streams whose token falls within the series of specified ranges.
func (i *Ingester) SendChunkRanges(ctx context.Context, ranges []ring.TokenRange, targetAddr string) error {
	// Block the ranges locally so we don't receive traffic for it anymore.
	i.BlockRanges(ranges)

	userStatesCopy := i.userStates.cp()
	if len(userStatesCopy) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, fakeOrgID)
	stream, err := c.TransferChunksSubset(ctx)
	if err != nil {
		return errors.Wrap(err, "SendChunks")
	}

	err = i.pushChunksToStream(pushChunksOptions{
		States:        userStatesCopy,
		Stream:        stream,
		MarkAsFlushed: true,

		IncludeFunc: func(pair fingerprintSeriesPair) bool {
			return inAnyRange(pair.series.token, ranges)
		},
	})
	if err != nil {
		return err
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "CloseAndRecv")
	}

	return nil
}

// RequestChunkRanges connects to the ingester at targetAddr and requests all
// chunks for streams whose fingerprint falls within the specified token
// ranges.
//
// If move is true, the target ingester should remove sent chunks from
// local memory if the transfer succeeds.
func (i *Ingester) RequestChunkRanges(ctx context.Context, ranges []ring.TokenRange, targetAddr string, move bool) error {
	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, fakeOrgID)
	stream, err := c.GetChunksSubset(ctx, &client.GetChunksRequest{
		Ranges:         ranges,
		Move:           move,
		FromIngesterId: i.lifecycler.ID,
	})
	if err != nil {
		return errors.Wrap(err, "GetChunks")
	}

	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	_, _, err = i.acceptChunksFromStream(acceptChunksOptions{
		States: i.userStates,
		Stream: stream,
	})
	if err != nil {
		return err
	}

	return nil
}

// RequestComplete connects to the ingester at targetAddr and calls
// CleanupTransfer.
func (i *Ingester) RequestComplete(ctx context.Context, ranges []ring.TokenRange, targetAddr string) {
	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "could not clean up target after tranfser", "err", err)
		return
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, fakeOrgID)
	_, err = c.UnblockRanges(ctx, &client.UnblockRangesRequest{Ranges: ranges})
	if err != nil {
		level.Error(util.Logger).Log("msg", "could not clean up target after transfer", "err", err)
	}
}

// MemoryStreamTokens returns series of tokens for in-memory streams.
func (i *Ingester) MemoryStreamTokens() []uint32 {
	var ret []uint32

	for _, state := range i.userStates.cp() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)

			// Skip when there's no chunks in a series. Used to avoid a panic on
			// calling head.
			if len(pair.series.chunkDescs) == 0 {
				state.fpLocker.Unlock(pair.fp)
				continue
			}

			if head := pair.series.head(); head != nil && !head.flushed {
				ret = append(ret, pair.series.token)
			}

			state.fpLocker.Unlock(pair.fp)
		}
	}

	return ret
}

func inAnyRange(tok uint32, ranges []ring.TokenRange) bool {
	for _, rg := range ranges {
		if rg.Contains(tok) {
			return true
		}
	}
	return false
}
