package ingester

import (
	"io"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"

	"github.com/weaveworks/common/user"

	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
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
	if err := i.ChangeState(ring.JOINING); err != nil {
		return err
	}

	// The ingesters state effectively works as a giant mutex around this whole
	// method, and as such we have to ensure we unlock the mutex.
	defer func() {
		errc := make(chan error)
		i.actorChan <- func() {
			if i.state == ring.JOINING {
				errc <- i.changeState(ring.PENDING)
			} else {
				errc <- nil
			}
		}
		if err := <-errc; err != nil {
			level.Error(util.Logger).Log("msg", "error rolling back failed TransferChunks", "err", err)
			os.Exit(1)
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

			if err := i.PreClaimTokensFor(fromIngesterID); err != nil {
				return err
			}
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

	if err := i.ClaimTokensFor(fromIngesterID); err != nil {
		return err
	}

	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	if err := i.ChangeState(ring.ACTIVE); err != nil {
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
