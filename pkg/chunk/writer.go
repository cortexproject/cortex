package chunk

import (
	"context"
	"flag"
	"sync"

	"github.com/go-kit/kit/log/level"
	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util"
)

// WriterConfig configures a Writer
type WriterConfig struct {
	writers      int     // Number of writers to run in parallel
	maxQueueSize int     // Max rows to allow in writer queue
	rateLimit    float64 // Max rate to send rows to storage back-end, per writer
	// (In some sense the rate we send should relate to the provisioned capacity in the back-end)
}

// Writer batches up writes to a Store
type Writer struct {
	WriterConfig

	storage StorageClient

	group   sync.WaitGroup
	pending sync.WaitGroup

	// Clients send items on this chan to be written
	Write chan WriteBatch
	// internally we send items on this chan to be retried
	retry chan WriteBatch
	// Writers read batches of items from this chan
	batched chan WriteBatch
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *WriterConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.writers, "writers", 1, "Number of writers to run in parallel")
	f.IntVar(&cfg.maxQueueSize, "writers-queue-limit", 1000, "Max rows to allow in writer queue")
	f.Float64Var(&cfg.rateLimit, "writer-rate-limit", 1000, "Max rate to send rows to storage back-end, per writer")
}

// NewWriter creates a new Writer
func NewWriter(cfg WriterConfig, storage StorageClient) *Writer {
	writer := &Writer{
		WriterConfig: cfg,
		storage:      storage,
		// Unbuffered chan so we can tell when batcher has received all items
		Write:   make(chan WriteBatch),
		retry:   make(chan WriteBatch, 100), // we should always accept retry data, to avoid deadlock
		batched: make(chan WriteBatch),
	}
	writer.Run()
	return writer
}

// IndexChunk writes the index entries for a chunk to the store
func (c *store) IndexChunk(ctx context.Context, chunk Chunk) error {
	writeReqs, err := c.calculateIndexEntries(chunk.UserID, chunk.From, chunk.Through, chunk)
	if err != nil {
		return err
	}
	c.writer.Write <- writeReqs
	return nil
}

// Run starts all the goroutines
func (writer *Writer) Run() {
	writer.group.Add(1 + writer.writers)
	go func() {
		writer.batcher()
		writer.group.Done()
	}()
	for i := 0; i < writer.writers; i++ {
		go func() {
			writer.writeLoop(context.TODO())
			writer.group.Done()
		}()
	}
}

// Flush all pending items to the backing store
// Note: nothing should be sent to the Write chan until this is over
func (writer *Writer) Flush() {
	// Ensure that batcher has received all items so it won't call Add() any more
	writer.Write <- nil
	// Wait for pending items to be sent to store
	writer.pending.Wait()
}

// Stop all background goroutines after flushing pending writes
func (writer *Writer) Stop() {
	writer.Flush()
	// Close chans to signal writer(s) and batcher to terminate
	close(writer.batched)
	close(writer.retry)
	writer.group.Wait()
}

// writeLoop receives on the 'batched' chan, sends to store, and
// passes anything that was throttled to the 'retry' chan.
func (writer *Writer) writeLoop(ctx context.Context) {
	limiter := rate.NewLimiter(rate.Limit(writer.rateLimit), 100) // burst size should be the largest batch
	for {
		batch, ok := <-writer.batched
		if !ok {
			return
		}
		batchLen := batch.Len()
		level.Debug(util.Logger).Log("msg", "about to write", "num_requests", batchLen)
		retry, err := writer.storage.BatchWriteNoRetry(ctx, batch)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to write; dropping data", "err", err, "batch", batch)
			writer.pending.Add(-batchLen)
			continue
		}
		if retry != nil {
			// Wait before retry
			limiter.WaitN(ctx, retry.Len())
			// Send unprocessed items back into the batcher
			writer.retry <- retry
		}
		writer.pending.Add(-(batchLen - retry.Len()))
	}
}

// Receive individual requests, and batch them up into groups to send to the store
func (writer *Writer) batcher() {
	flushing := false
	var queue, outBatch WriteBatch
	queue = writer.storage.NewWriteBatch()
	for {
		queueLen := queue.Len()
		writerQueueLength.Set(float64(queueLen)) // Prometheus metric
		var in, out chan WriteBatch
		// We will allow in new data if the queue isn't too long
		if queueLen < writer.maxQueueSize {
			in = writer.Write
		}
		// We will send out a batch if the queue is big enough, or if we're flushing
		if outBatch == nil || outBatch.Len() == 0 {
			var removed int
			outBatch, removed = queue.Take(flushing)
			if removed > 0 {
				// Account for entries removed from the queue which are not in the batch
				// (e.g. because of deduplication)
				writer.pending.Add(-(removed - outBatch.Len()))
			}
		}
		if outBatch != nil && outBatch.Len() > 0 {
			out = writer.batched
		}
		select {
		case inBatch := <-in:
			if inBatch == nil { // Nil used as interlock to know we received all previous values
				flushing = true
			} else {
				flushing = false
				queue.AddBatch(inBatch)
				writer.pending.Add(inBatch.Len())
			}
		case inBatch, ok := <-writer.retry:
			if !ok {
				return
			}
			queue.AddBatch(inBatch)
		case out <- outBatch:
			outBatch = nil
		}
	}
}
