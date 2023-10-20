package storegateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/storegateway/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/weaveworks/common/httpgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

type loadIdx struct {
	offset uint32
	// Indices, not actual entries and chunks.
	seriesEntry int
	chunk       int
}

type bucketChunkReader struct {
	block *bucketBlock

	toLoad [][]loadIdx

	// chunkBytesMtx protects access to chunkBytes, when updated from chunks-loading goroutines.
	// After chunks are loaded, mutex is no longer used.
	chunkBytesMtx sync.Mutex
	stats         *queryStats
	chunkBytes    []*[]byte // Byte slice to return to the chunk pool on close.

	loadingChunksMtx  sync.Mutex
	loadingChunks     bool
	finishLoadingChks chan struct{}
}

func newBucketChunkReader(block *bucketBlock) *bucketChunkReader {
	return &bucketChunkReader{
		block:  block,
		stats:  &queryStats{},
		toLoad: make([][]loadIdx, len(block.chunkObjs)),
	}
}

func (r *bucketChunkReader) reset() {
	for i := range r.toLoad {
		r.toLoad[i] = r.toLoad[i][:0]
	}
	r.loadingChunksMtx.Lock()
	r.loadingChunks = false
	r.finishLoadingChks = make(chan struct{})
	r.loadingChunksMtx.Unlock()
}

func (r *bucketChunkReader) Close() error {
	// NOTE(GiedriusS): we need to wait until loading chunks because loading
	// chunks modifies r.block.chunkPool.
	r.loadingChunksMtx.Lock()
	loadingChks := r.loadingChunks
	r.loadingChunksMtx.Unlock()

	if loadingChks {
		<-r.finishLoadingChks
	}
	r.block.pendingReaders.Done()

	for _, b := range r.chunkBytes {
		r.block.chunkPool.Put(b)
	}
	return nil
}

// addLoad adds the chunk with id to the data set to be fetched.
// Chunk will be fetched and saved to refs[seriesEntry][chunk] upon r.load(refs, <...>) call.
func (r *bucketChunkReader) addLoad(id chunks.ChunkRef, seriesEntry, chunk int) error {
	var (
		seq = int(id >> 32)
		off = uint32(id)
	)
	if seq >= len(r.toLoad) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.toLoad[seq] = append(r.toLoad[seq], loadIdx{off, seriesEntry, chunk})
	return nil
}

// load loads all added chunks and saves resulting aggrs to refs.
func (r *bucketChunkReader) load(ctx context.Context, res []seriesEntry, aggrs []storepb.Aggr, calculateChunkChecksum bool, bytesLimiter BytesLimiter, tenant string) error {
	r.loadingChunksMtx.Lock()
	r.loadingChunks = true
	r.loadingChunksMtx.Unlock()

	begin := time.Now()
	defer func() {
		r.stats.ChunksDownloadLatencySum += time.Since(begin)

		r.loadingChunksMtx.Lock()
		r.loadingChunks = false
		r.loadingChunksMtx.Unlock()

		close(r.finishLoadingChks)
	}()

	g, ctx := errgroup.WithContext(ctx)

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioner.Partition(len(pIdxs), func(i int) (start, end uint64) {
			return uint64(pIdxs[i].offset), uint64(pIdxs[i].offset) + uint64(r.block.estimatedMaxChunkSize)
		})

		for _, p := range parts {
			if err := bytesLimiter.Reserve(uint64(p.End - p.Start)); err != nil {
				return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while fetching chunks: %s", err)
			}
			r.stats.DataDownloadedSizeSum += units.Base2Bytes(p.End - p.Start)
		}

		for _, p := range parts {
			seq := seq
			p := p
			indices := pIdxs[p.ElemRng[0]:p.ElemRng[1]]
			g.Go(func() error {
				return r.loadChunks(ctx, res, aggrs, seq, p, indices, calculateChunkChecksum, bytesLimiter, tenant)
			})
		}
	}
	return g.Wait()
}

// loadChunks will read range [start, end] from the segment file with sequence number seq.
// This data range covers chunks starting at supplied offsets.
func (r *bucketChunkReader) loadChunks(ctx context.Context, res []seriesEntry, aggrs []storepb.Aggr, seq int, part Part, pIdxs []loadIdx, calculateChunkChecksum bool, bytesLimiter BytesLimiter, tenant string) error {
	fetchBegin := time.Now()
	stats := new(queryStats)
	defer func() {
		stats.ChunksFetchDurationSum += time.Since(fetchBegin)
		r.stats.merge(stats)
	}()

	// Get a reader for the required range.
	reader, err := r.block.chunkRangeReader(ctx, seq, int64(part.Start), int64(part.End-part.Start))
	if err != nil {
		return errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, reader, "readChunkRange close range reader")
	bufReader := bufio.NewReaderSize(reader, r.block.estimatedMaxChunkSize)

	stats.chunksFetchCount++
	stats.chunksFetched += len(pIdxs)
	stats.ChunksFetchedSizeSum += units.Base2Bytes(int(part.End - part.Start))

	var (
		buf        []byte
		readOffset = int(pIdxs[0].offset)

		// Save a few allocations.
		written  int
		diff     uint32
		chunkLen int
		n        int
	)

	bufPooled, err := r.block.chunkPool.Get(r.block.estimatedMaxChunkSize)
	if err == nil {
		buf = *bufPooled
	} else {
		buf = make([]byte, r.block.estimatedMaxChunkSize)
	}
	defer r.block.chunkPool.Put(&buf)

	for i, pIdx := range pIdxs {
		// Fast forward range reader to the next chunk start in case of sparse (for our purposes) byte range.
		for readOffset < int(pIdx.offset) {
			written, err = bufReader.Discard(int(pIdx.offset) - int(readOffset))
			if err != nil {
				return errors.Wrap(err, "fast forward range reader")
			}
			readOffset += written
		}
		// Presume chunk length to be reasonably large for common use cases.
		// However, declaration for EstimatedMaxChunkSize warns us some chunks could be larger in some rare cases.
		// This is handled further down below.
		chunkLen = r.block.estimatedMaxChunkSize
		if i+1 < len(pIdxs) {
			if diff = pIdxs[i+1].offset - pIdx.offset; int(diff) < chunkLen {
				chunkLen = int(diff)
			}
		}
		cb := buf[:chunkLen]
		n, err = io.ReadFull(bufReader, cb)
		readOffset += n
		// Unexpected EOF for last chunk could be a valid case. Any other errors are definitely real.
		if err != nil && !(errors.Is(err, io.ErrUnexpectedEOF) && i == len(pIdxs)-1) {
			return errors.Wrapf(err, "read range for seq %d offset %x", seq, pIdx.offset)
		}

		chunkDataLen, n := binary.Uvarint(cb)
		if n < 1 {
			return errors.New("reading chunk length failed")
		}

		// Chunk length is n (number of bytes used to encode chunk data), 1 for chunk encoding and chunkDataLen for actual chunk data.
		// There is also crc32 after the chunk, but we ignore that.
		chunkLen = n + 1 + int(chunkDataLen)
		if chunkLen <= len(cb) {
			err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), rawChunk(cb[n:chunkLen]), aggrs, r.save, calculateChunkChecksum)
			if err != nil {
				return errors.Wrap(err, "populate chunk")
			}
			stats.chunksTouched++
			stats.ChunksTouchedSizeSum += units.Base2Bytes(int(chunkDataLen))
			continue
		}

		r.block.metrics.chunkRefetches.WithLabelValues(tenant).Inc()
		// If we didn't fetch enough data for the chunk, fetch more.
		fetchBegin = time.Now()
		// Read entire chunk into new buffer.
		// TODO: readChunkRange call could be avoided for any chunk but last in this particular part.
		if err := bytesLimiter.Reserve(uint64(chunkLen)); err != nil {
			return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while fetching chunks: %s", err)
		}
		stats.DataDownloadedSizeSum += units.Base2Bytes(chunkLen)

		nb, err := r.block.readChunkRange(ctx, seq, int64(pIdx.offset), int64(chunkLen), []byteRange{{offset: 0, length: chunkLen}})
		if err != nil {
			return errors.Wrapf(err, "preloaded chunk too small, expecting %d, and failed to fetch full chunk", chunkLen)
		}
		if len(*nb) != chunkLen {
			return errors.Errorf("preloaded chunk too small, expecting %d", chunkLen)
		}

		stats.chunksFetchCount++
		stats.ChunksFetchedSizeSum += units.Base2Bytes(len(*nb))
		err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), rawChunk((*nb)[n:]), aggrs, r.save, calculateChunkChecksum)
		if err != nil {
			r.block.chunkPool.Put(nb)
			return errors.Wrap(err, "populate chunk")
		}
		stats.chunksTouched++
		stats.ChunksTouchedSizeSum += units.Base2Bytes(int(chunkDataLen))

		r.block.chunkPool.Put(nb)
	}
	return nil
}

// save saves a copy of b's payload to a memory pool of its own and returns a new byte slice referencing said copy.
// Returned slice becomes invalid once r.block.chunkPool.Put() is called.
func (r *bucketChunkReader) save(b []byte) ([]byte, error) {
	r.chunkBytesMtx.Lock()
	defer r.chunkBytesMtx.Unlock()
	// Ensure we never grow slab beyond original capacity.
	if len(r.chunkBytes) == 0 ||
		cap(*r.chunkBytes[len(r.chunkBytes)-1])-len(*r.chunkBytes[len(r.chunkBytes)-1]) < len(b) {
		s, err := r.block.chunkPool.Get(len(b))
		if err != nil {
			return nil, errors.Wrap(err, "allocate chunk bytes")
		}
		r.chunkBytes = append(r.chunkBytes, s)
	}
	slab := r.chunkBytes[len(r.chunkBytes)-1]
	*slab = append(*slab, b...)
	return (*slab)[len(*slab)-len(b):], nil
}

// rawChunk is a helper type that wraps a chunk's raw bytes and implements the chunkenc.Chunk
// interface over it.
// It is used to Store API responses which don't need to introspect and validate the chunk's contents.
type rawChunk []byte

func (b rawChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding(b[0])
}

func (b rawChunk) Bytes() []byte {
	return b[1:]
}
func (b rawChunk) Compact() {}

func (b rawChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	panic("invalid call")
}

func (b rawChunk) Appender() (chunkenc.Appender, error) {
	panic("invalid call")
}

func (b rawChunk) NumSamples() int {
	panic("invalid call")
}
