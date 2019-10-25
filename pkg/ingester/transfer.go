package ingester

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/shipper"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/user"
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
	sentFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_sent_files",
		Help: "The total number of files sent by this ingester whilst leaving.",
	})
	receivedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_received_files",
		Help: "The total number of files received by this ingester whilst joining",
	})

	once *sync.Once
)

func init() {
	once = &sync.Once{}
	prometheus.MustRegister(sentChunks)
	prometheus.MustRegister(receivedChunks)
	prometheus.MustRegister(sentFiles)
	prometheus.MustRegister(receivedFiles)
}

// TransferChunks receives all the chunks from another ingester.
func (i *Ingester) TransferChunks(stream client.Ingester_TransferChunksServer) error {
	fromIngesterID := ""
	seriesReceived := 0
	xfer := func() error {
		userStates := newUserStates(i.limits, i.cfg)

		for {
			wireSeries, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "TransferChunks: Recv")
			}

			// We can't send "extra" fields with a streaming call, so we repeat
			// wireSeries.FromIngesterId and assume it is the same every time
			// round this loop.
			if fromIngesterID == "" {
				fromIngesterID = wireSeries.FromIngesterId
				level.Info(util.Logger).Log("msg", "processing TransferChunks request", "from_ingester", fromIngesterID)
			}
			descs, err := fromWireChunks(wireSeries.Chunks)
			if err != nil {
				return errors.Wrap(err, "TransferChunks: fromWireChunks")
			}

			state, fp, series, err := userStates.getOrCreateSeries(stream.Context(), wireSeries.UserId, wireSeries.Labels)
			if err != nil {
				return errors.Wrapf(err, "TransferChunks: getOrCreateSeries: user %s series %s", wireSeries.UserId, wireSeries.Labels)
			}
			prevNumChunks := len(series.chunkDescs)

			err = series.setChunks(descs)
			state.fpLocker.Unlock(fp) // acquired in getOrCreateSeries
			if err != nil {
				return errors.Wrapf(err, "TransferChunks: setChunks: user %s series %s", wireSeries.UserId, wireSeries.Labels)
			}

			seriesReceived++
			memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
			receivedChunks.Add(float64(len(descs)))
		}

		if seriesReceived == 0 {
			level.Error(util.Logger).Log("msg", "received TransferChunks request with no series", "from_ingester", fromIngesterID)
			return fmt.Errorf("TransferChunks: no series")
		}

		if fromIngesterID == "" {
			level.Error(util.Logger).Log("msg", "received TransferChunks request with no ID from ingester")
			return fmt.Errorf("no ingester id")
		}

		if err := i.lifecycler.ClaimTokensFor(stream.Context(), fromIngesterID); err != nil {
			return errors.Wrap(err, "TransferChunks: ClaimTokensFor")
		}

		i.userStatesMtx.Lock()
		defer i.userStatesMtx.Unlock()

		i.userStates = userStates

		return nil
	}

	if err := i.transfer(stream.Context(), xfer); err != nil {
		return err
	}

	// Close the stream last, as this is what tells the "from" ingester that
	// it's OK to shut down.
	if err := stream.SendAndClose(&client.TransferChunksResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing TransferChunks stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}
	level.Info(util.Logger).Log("msg", "Successfully transferred chunks", "from_ingester", fromIngesterID, "series_received", seriesReceived)

	return nil
}

func (i *Ingester) transfer(ctx context.Context, xfer func() error) error {
	// Enter JOINING state (only valid from PENDING)
	if err := i.lifecycler.ChangeState(ctx, ring.JOINING); err != nil {
		return err
	}

	// The ingesters state effectively works as a giant mutex around this whole
	// method, and as such we have to ensure we unlock the mutex.
	defer func() {
		state := i.lifecycler.GetState()
		if i.lifecycler.GetState() == ring.ACTIVE {
			return
		}

		level.Error(util.Logger).Log("msg", "TransferChunks failed, not in ACTIVE state.", "state", state)

		// Enter PENDING state (only valid from JOINING)
		if i.lifecycler.GetState() == ring.JOINING {
			if err := i.lifecycler.ChangeState(ctx, ring.PENDING); err != nil {
				level.Error(util.Logger).Log("msg", "error rolling back failed TransferChunks", "err", err)
				os.Exit(1)
			}
		}
	}()

	if err := xfer(); err != nil {
		return err
	}

	if err := i.lifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return errors.Wrap(err, "Transfer: ChangeState")
	}

	return nil
}

// TransferTSDB receives all the file chunks from another ingester, and writes them to tsdb directories
func (i *Ingester) TransferTSDB(stream client.Ingester_TransferTSDBServer) error {
	fromIngesterID := ""

	xfer := func() error {
		filesXfer := 0

		files := make(map[string]*os.File)
		defer func() {
			for _, f := range files {
				if err := f.Close(); err != nil {
					level.Warn(util.Logger).Log("msg", "failed to close xfer file", "err", err)
				}
			}
		}()
		for {
			f, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "TransferTSDB: Recv")
			}
			if fromIngesterID == "" {
				fromIngesterID = f.FromIngesterId
				level.Info(util.Logger).Log("msg", "processing TransferTSDB request", "from_ingester", fromIngesterID)
			}
			filesXfer++

			// TODO(thor) To avoid corruption from errors, it's probably best to write to a temp dir, and then move that to the final location
			createfile := func(f *client.TimeSeriesFile) (*os.File, error) {
				dir := filepath.Join(i.cfg.TSDBConfig.Dir, filepath.Dir(f.Filename))
				if err := os.MkdirAll(dir, 0777); err != nil {
					return nil, errors.Wrap(err, "TransferTSDB: MkdirAll")
				}
				file, err := os.Create(filepath.Join(i.cfg.TSDBConfig.Dir, f.Filename))
				if err != nil {
					return nil, errors.Wrap(err, "TransferTSDB: Create")
				}

				_, err = file.Write(f.Data)
				return file, errors.Wrap(err, "TransferTSDB: Write")
			}

			// Create or get existing open file
			file, ok := files[f.Filename]
			if !ok {
				file, err = createfile(f)
				if err != nil {
					return err
				}

				files[f.Filename] = file
			} else {

				// Write to existing file
				if _, err := file.Write(f.Data); err != nil {
					return errors.Wrap(err, "TransferTSDB: Write")
				}
			}
		}

		if err := i.lifecycler.ClaimTokensFor(stream.Context(), fromIngesterID); err != nil {
			return errors.Wrap(err, "TransferTSDB: ClaimTokensFor")
		}

		receivedFiles.Add(float64(filesXfer))
		level.Error(util.Logger).Log("msg", "Total files xfer", "from_ingester", fromIngesterID, "num", filesXfer)

		return nil
	}

	if err := i.transfer(stream.Context(), xfer); err != nil {
		return err
	}

	// Close the stream last, as this is what tells the "from" ingester that
	// it's OK to shut down.
	if err := stream.SendAndClose(&client.TransferTSDBResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing TransferTSDB stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}
	level.Info(util.Logger).Log("msg", "Successfully transferred tsdbs", "from_ingester", fromIngesterID)

	return nil
}

func toWireChunks(descs []*desc) ([]client.Chunk, error) {
	wireChunks := make([]client.Chunk, 0, len(descs))
	for _, d := range descs {
		wireChunk := client.Chunk{
			StartTimestampMs: int64(d.FirstTime),
			EndTimestampMs:   int64(d.LastTime),
			Encoding:         int32(d.C.Encoding()),
		}

		buf := bytes.NewBuffer(make([]byte, 0, d.C.Size()))
		if err := d.C.Marshal(buf); err != nil {
			return nil, err
		}

		wireChunk.Data = buf.Bytes()
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
		desc.C, err = encoding.NewForEncoding(encoding.Encoding(byte(c.Encoding)))
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
func (i *Ingester) TransferOut(ctx context.Context) error {
	if i.cfg.MaxTransferRetries < 0 {
		return fmt.Errorf("transfers disabled")
	}
	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
		MaxRetries: i.cfg.MaxTransferRetries,
	})

	for backoff.Ongoing() {
		err := i.transferOut(ctx)
		if err == nil {
			return nil
		}

		level.Error(util.Logger).Log("msg", "transfer failed", "err", err)
		backoff.Wait()
	}

	return backoff.Err()
}

func (i *Ingester) transferOut(ctx context.Context) error {
	if i.cfg.TSDBEnabled {
		return i.v2TransferOut(ctx)
	}

	userStatesCopy := i.userStates.cp()
	if len(userStatesCopy) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	targetIngester, err := i.findTargetIngester(ctx)
	if err != nil {
		return fmt.Errorf("cannot find ingester to transfer chunks to: %v", err)
	}

	level.Info(util.Logger).Log("msg", "sending chunks", "to_ingester", targetIngester.Addr)
	c, err := i.cfg.ingesterClientFactory(targetIngester.Addr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	stream, err := c.TransferChunks(ctx)
	if err != nil {
		return errors.Wrap(err, "TransferChunks")
	}

	for userID, state := range userStatesCopy {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)

			if len(pair.series.chunkDescs) == 0 { // Nothing to send?
				state.fpLocker.Unlock(pair.fp)
				continue
			}

			chunks, err := toWireChunks(pair.series.chunkDescs)
			if err != nil {
				state.fpLocker.Unlock(pair.fp)
				return errors.Wrap(err, "toWireChunks")
			}

			err = stream.Send(&client.TimeSeriesChunk{
				FromIngesterId: i.lifecycler.ID,
				UserId:         userID,
				Labels:         client.FromLabelsToLabelAdapters(pair.series.metric),
				Chunks:         chunks,
			})
			state.fpLocker.Unlock(pair.fp)
			if err != nil {
				return errors.Wrap(err, "Send")
			}

			sentChunks.Add(float64(len(chunks)))
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "CloseAndRecv")
	}

	// Close & empty all the flush queues, to unblock waiting workers.
	for _, flushQueue := range i.flushQueues {
		flushQueue.DiscardAndClose()
	}
	i.flushQueuesDone.Wait()

	level.Info(util.Logger).Log("msg", "successfully sent chunks", "to_ingester", targetIngester.Addr)
	return nil
}

func (i *Ingester) v2TransferOut(ctx context.Context) error {
	if len(i.TSDBState.dbs) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	// Close all user databases
	wg := &sync.WaitGroup{}
	// Only perform a shutdown once
	once.Do(func() {
		wg.Add(len(i.TSDBState.dbs))
		for _, db := range i.TSDBState.dbs {
			go func(closer io.Closer) {
				defer wg.Done()
				if err := closer.Close(); err != nil {
					level.Warn(util.Logger).Log("msg", "failed to close db", "err", err)
				}
			}(db)
		}
	})

	targetIngester, err := i.findTargetIngester(ctx)
	if err != nil {
		return fmt.Errorf("cannot find ingester to transfer blocks to: %v", err)
	}

	level.Info(util.Logger).Log("msg", "sending blocks", "to_ingester", targetIngester.Addr)
	c, err := i.cfg.ingesterClientFactory(targetIngester.Addr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	stream, err := c.TransferTSDB(ctx)
	if err != nil {
		return errors.Wrap(err, "TransferTSDB")
	}

	wg.Wait() // wait for all databases to have closed

	// Grab a list of all blocks that need to be shipped
	blocks, err := unshippedBlocks(i.cfg.TSDBConfig.Dir)
	if err != nil {
		return err
	}

	for user, blockIDs := range blocks {
		// Transfer the users TSDB
		// TODO(thor) transferring users can be done concurrently
		transferUser(ctx, stream, i.cfg.TSDBConfig.Dir, i.lifecycler.ID, user, blockIDs)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "CloseAndRecv")
	}

	return nil
}

// findTargetIngester finds an ingester in PENDING state.
func (i *Ingester) findTargetIngester(ctx context.Context) (*ring.IngesterDesc, error) {
	ringDesc, err := i.lifecycler.KVStore.Get(ctx, ring.ConsulKey)
	if err != nil {
		return nil, err
	}

	ingesters := ringDesc.(*ring.Desc).FindIngestersByState(ring.PENDING)
	if len(ingesters) <= 0 {
		return nil, fmt.Errorf("no pending ingesters")
	}

	return &ingesters[0], nil
}

// unshippedBlocks returns a ulid list of blocks that haven't been shipped
func unshippedBlocks(dir string) (map[string][]string, error) {
	userIDs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	blocks := make(map[string][]string, len(userIDs))
	for _, user := range userIDs {
		userID := user.Name()
		blocks[userID] = []string{} // seed the map with the userID to ensure we xfer the WAL, even if all blocks are shipped

		blockIDs, err := ioutil.ReadDir(filepath.Join(dir, userID))
		if err != nil {
			return nil, err
		}

		m, err := shipper.ReadMetaFile(filepath.Join(dir, userID))
		if err != nil {
			return nil, err
		}

		shipped := make(map[string]bool)
		for _, u := range m.Uploaded {
			shipped[u.String()] = true
		}

		for _, blockID := range blockIDs {
			_, err := ulid.Parse(blockID.Name())
			if err != nil {
				continue
			}

			if _, ok := shipped[blockID.Name()]; !ok {
				blocks[userID] = append(blocks[userID], blockID.Name())
			}
		}
	}

	return blocks, nil
}

func transferUser(ctx context.Context, stream client.Ingester_TransferTSDBClient, dir, ingesterID, userID string, blocks []string) {
	level.Info(util.Logger).Log("msg", "xfer user", "user", userID)
	// Transfer all blocks
	for _, blk := range blocks {
		err := filepath.Walk(filepath.Join(dir, userID, blk), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}

			if info.IsDir() {
				return nil
			}

			b, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			p, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}

			if err := batchSend(1024*1024, b, stream, &client.TimeSeriesFile{
				FromIngesterId: ingesterID,
				UserId:         userID,
				Filename:       p,
			}); err != nil {
				return err
			}

			sentFiles.Add(1)
			return nil
		})
		if err != nil {
			level.Warn(util.Logger).Log("msg", "failed to transfer all user blocks", "err", err)
		}
	}

	// Transfer WAL
	err := filepath.Walk(filepath.Join(dir, userID, "wal"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if info.IsDir() {
			return nil
		}

		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		p, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		if err := batchSend(1024*1024, b, stream, &client.TimeSeriesFile{
			FromIngesterId: ingesterID,
			UserId:         userID,
			Filename:       p,
		}); err != nil {
			return err
		}

		sentFiles.Add(1)
		return nil
	})

	if err != nil {
		level.Warn(util.Logger).Log("msg", "failed to transfer user wal", "err", err)
	}

	level.Info(util.Logger).Log("msg", "xfer user complete", "user", userID)
}

func batchSend(batch int, b []byte, stream client.Ingester_TransferTSDBClient, tsfile *client.TimeSeriesFile) error {
	// Split file into smaller blocks for xfer
	i := 0
	for ; i+batch < len(b); i += batch {
		tsfile.Data = b[i : i+batch]
		err := stream.Send(tsfile)
		if err != nil {
			return err
		}
	}

	// Send final data
	if i < len(b) {
		tsfile.Data = b[i:]
		err := stream.Send(tsfile)
		if err != nil {
			return err
		}
	}

	return nil
}
