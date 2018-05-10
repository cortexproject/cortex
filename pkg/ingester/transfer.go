package ingester

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	pendingSearchIterations = 10
)

var (
	sentChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_sent_chunks",
		Help: "The total number of chunks sent by this ingester whilst leaving.",
	})
	receivedChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_received_chunks",
		Help: "The total number of chunks received by this ingester whilst joining",
	})
)

func init() {
	prometheus.MustRegister(sentChunks)
	prometheus.MustRegister(receivedChunks)
}

// TransferChunks receives all the chunks from another ingester.
func (i *Ingester) TransferChunks(stream client.Ingester_TransferChunksServer) error {
	// Enter JOINING state (only valid from PENDING)
	if err := i.lifecycler.ChangeState(ring.JOINING); err != nil {
		return err
	}

	// The ingesters state effectively works as a giant mutex around this whole
	// method, and as such we have to ensure we unlock the mutex.
	defer func() {
		state := i.lifecycler.GetState()
		if i.lifecycler.GetState() == ring.ACTIVE {
			return
		}

		level.Error(util.Logger).Log("msg", "TranferChunks failed, not in ACTIVE state.", "state", state)

		// Enter PENDING state (only valid from JOINING)
		if i.lifecycler.GetState() == ring.JOINING {
			if err := i.lifecycler.ChangeState(ring.PENDING); err != nil {
				level.Error(util.Logger).Log("msg", "error rolling back failed TransferChunks", "err", err)
				os.Exit(1)
			}
		}
	}()

	userStates := newUserStates(&i.cfg.userStatesConfig)
	fromIngesterID := ""

	for {
		wireSeries, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// We can't send "extra" fields with a streaming call, so we repeat
		// wireSeries.FromIngesterId and assume it is the same every time
		// round this loop.
		if fromIngesterID == "" {
			fromIngesterID = wireSeries.FromIngesterId
			level.Info(util.Logger).Log("msg", "processing TransferChunks request", "from_ingester", fromIngesterID)
		}
		metric := client.FromLabelPairs(wireSeries.Labels)
		userCtx := user.InjectOrgID(stream.Context(), wireSeries.UserId)
		descs, err := fromWireChunks(wireSeries.Chunks)
		if err != nil {
			return err
		}

		state, fp, series, err := userStates.getOrCreateSeries(userCtx, metric)
		if err != nil {
			return err
		}
		prevNumChunks := len(series.chunkDescs)

		err = series.setChunks(descs)
		state.fpLocker.Unlock(fp) // acquired in getOrCreateSeries
		if err != nil {
			return err
		}

		i.memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
		receivedChunks.Add(float64(len(descs)))
	}

	if err := i.lifecycler.ClaimTokensFor(fromIngesterID); err != nil {
		return err
	}

	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	if err := i.lifecycler.ChangeState(ring.ACTIVE); err != nil {
		return err
	}
	i.userStates = userStates

	// Close the stream last, as this is what tells the "from" ingester that
	// it's OK to shut down.
	if err := stream.SendAndClose(&client.TransferChunksResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing TransferChunks stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}
	level.Info(util.Logger).Log("msg", "Successfully transferred chunks", "from_ingester", fromIngesterID)
	return nil
}

func toWireChunks(descs []*desc) ([]client.Chunk, error) {
	wireChunks := make([]client.Chunk, 0, len(descs))
	for _, d := range descs {
		wireChunk := client.Chunk{
			StartTimestampMs: int64(d.FirstTime),
			EndTimestampMs:   int64(d.LastTime),
			Encoding:         int32(d.C.Encoding()),
			Data:             make([]byte, chunk.ChunkLen, chunk.ChunkLen),
		}

		if err := d.C.MarshalToBuf(wireChunk.Data); err != nil {
			return nil, err
		}

		wireChunks = append(wireChunks, wireChunk)
	}
	return wireChunks, nil
}

func fromWireChunks(wireChunks []client.Chunk) ([]*desc, error) {
	descs := make([]*desc, 0, len(wireChunks))
	for _, c := range wireChunks {
		desc := &desc{
			FirstTime:  model.Time(c.StartTimestampMs),
			LastTime:   model.Time(c.EndTimestampMs),
			LastUpdate: model.Now(),
		}

		var err error
		desc.C, err = chunk.NewForEncoding(chunk.Encoding(byte(c.Encoding)))
		if err != nil {
			return nil, err
		}

		if err := desc.C.UnmarshalFromBuf(c.Data); err != nil {
			return nil, err
		}

		descs = append(descs, desc)
	}
	return descs, nil
}

// TransferOut finds an ingester in PENDING state and transfers our chunks to it.
// Called as part of the ingester shutdown process.
func (i *Ingester) TransferOut() error {
	targetIngester, err := i.findTargetIngester()
	if err != nil {
		return fmt.Errorf("cannot find ingester to transfer chunks to: %v", err)
	}

	level.Info(util.Logger).Log("msg", "sending chunks", "to_ingester", targetIngester.Addr)
	c, err := i.cfg.ingesterClientFactory(targetIngester.Addr, i.cfg.clientConfig)
	if err != nil {
		return err
	}
	defer c.(io.Closer).Close()

	ctx := user.InjectOrgID(context.Background(), "-1")
	stream, err := c.TransferChunks(ctx)
	if err != nil {
		return err
	}

	for userID, state := range i.userStates.cp() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)

			if len(pair.series.chunkDescs) == 0 { // Nothing to send?
				state.fpLocker.Unlock(pair.fp)
				continue
			}

			chunks, err := toWireChunks(pair.series.chunkDescs)
			if err != nil {
				state.fpLocker.Unlock(pair.fp)
				return err
			}

			err = stream.Send(&client.TimeSeriesChunk{
				FromIngesterId: i.lifecycler.ID,
				UserId:         userID,
				Labels:         client.ToLabelPairs(pair.series.metric),
				Chunks:         chunks,
			})
			state.fpLocker.Unlock(pair.fp)
			if err != nil {
				return err
			}

			sentChunks.Add(float64(len(chunks)))
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return err
	}

	// Close & empty all the flush queues, to unblock waiting workers.
	for _, flushQueue := range i.flushQueues {
		flushQueue.DiscardAndClose()
	}
	i.flushQueuesDone.Wait()

	level.Info(util.Logger).Log("msg", "successfully sent chunks", "to_ingester", targetIngester.Addr)
	return nil
}

// findTargetIngester finds an ingester in PENDING state.
func (i *Ingester) findTargetIngester() (*ring.IngesterDesc, error) {
	findIngester := func() (*ring.IngesterDesc, error) {
		ringDesc, err := i.lifecycler.KVStore.Get(ring.ConsulKey)
		if err != nil {
			return nil, err
		}

		ingesters := ringDesc.(*ring.Desc).FindIngestersByState(ring.PENDING)
		if len(ingesters) <= 0 {
			return nil, fmt.Errorf("no pending ingesters")
		}

		return ingesters[0], nil
	}

	deadline := time.Now().Add(i.cfg.SearchPendingFor)
	for {
		ingester, err := findIngester()
		if err != nil {
			level.Debug(util.Logger).Log("msg", "Error looking for pending ingester", "err", err)
			if time.Now().Before(deadline) {
				time.Sleep(i.cfg.SearchPendingFor / pendingSearchIterations)
				continue
			} else {
				level.Warn(util.Logger).Log("msg", "Could not find pending ingester before deadline", "err", err)
				return nil, err
			}
		}
		return ingester, nil
	}
}
