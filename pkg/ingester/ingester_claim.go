package ingester

import (
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"

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
	log.Infof("Got TransferChunks request from ingester '%s'.")

	// Enter JOINING state (only valid from PENDING)
	if err := i.ChangeState(ring.JOINING); err != nil {
		return err
	}

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
			log.Infof("Processing TransferChunks request from ingester '%s'.")
		}
		metric := util.FromLabelPairs(wireSeries.Labels)
		userCtx := user.Inject(stream.Context(), wireSeries.UserId)
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
		sentChunks.Add(float64(len(descs)))
	}

	if err := stream.SendAndClose(&client.TransferChunksResponse{}); err != nil {
		return err
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
			FirstTime: model.Time(c.StartTimestampMs),
			LastTime:  model.Time(c.EndTimestampMs),
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
