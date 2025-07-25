package bucketindex

import (
	"context"
	"encoding/json"
	"io"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/runutil"
)

var (
	ErrBlockMetaNotFound          = block.ErrorSyncMetaNotFound
	ErrBlockMetaCorrupted         = block.ErrorSyncMetaCorrupted
	ErrBlockDeletionMarkNotFound  = errors.New("block deletion mark not found")
	ErrBlockDeletionMarkCorrupted = errors.New("block deletion mark corrupted")

	errBlockMetaKeyAccessDeniedErr = errors.New("block meta file key access denied error")
)

// Updater is responsible to generate an update in-memory bucket index.
type Updater struct {
	bkt            objstore.InstrumentedBucket
	logger         log.Logger
	parquetEnabled bool
}

func NewUpdater(bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *Updater {
	return &Updater{
		bkt:    bucket.NewUserBucketClient(userID, bkt, cfgProvider),
		logger: util_log.WithUserID(userID, logger),
	}
}

func (w *Updater) EnableParquet() *Updater {
	w.parquetEnabled = true
	return w
}

// UpdateIndex generates the bucket index and returns it, without storing it to the storage.
// If the old index is not passed in input, then the bucket index will be generated from scratch.
func (w *Updater) UpdateIndex(ctx context.Context, old *Index) (*Index, map[ulid.ULID]error, int64, error) {
	var (
		oldBlocks             []*Block
		oldBlockDeletionMarks []*BlockDeletionMark
	)

	// Read the old index, if provided.
	if old != nil {
		oldBlocks = old.Blocks
		oldBlockDeletionMarks = old.BlockDeletionMarks
	}

	blockDeletionMarks, deletedBlocks, totalBlocksBlocksMarkedForNoCompaction, err := w.updateBlockMarks(ctx, oldBlockDeletionMarks)
	if err != nil {
		return nil, nil, 0, err
	}

	blocks, partials, err := w.updateBlocks(ctx, oldBlocks, deletedBlocks)
	if err != nil {
		return nil, nil, 0, err
	}
	if w.parquetEnabled {
		if err := w.updateParquetBlocks(ctx, blocks); err != nil {
			return nil, nil, 0, err
		}
	}

	return &Index{
		Version:            IndexVersion1,
		Blocks:             blocks,
		BlockDeletionMarks: blockDeletionMarks,
		UpdatedAt:          time.Now().Unix(),
	}, partials, totalBlocksBlocksMarkedForNoCompaction, nil
}

func (w *Updater) updateBlocks(ctx context.Context, old []*Block, deletedBlocks map[ulid.ULID]struct{}) (blocks []*Block, partials map[ulid.ULID]error, _ error) {
	discovered := map[ulid.ULID]struct{}{}
	partials = map[ulid.ULID]error{}

	// Find all blocks in the storage.
	err := w.bkt.Iter(ctx, "", func(name string) error {
		if id, ok := block.IsBlockDir(name); ok {
			discovered[id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "list blocks")
	}

	// Since blocks are immutable, all blocks already existing in the index can just be copied.
	for _, b := range old {
		if _, ok := discovered[b.ID]; ok {
			delete(discovered, b.ID)

			if _, ok := deletedBlocks[b.ID]; ok {
				level.Warn(w.logger).Log("msg", "skipped block with missing global deletion marker", "block", b.ID.String())
				continue
			}
			blocks = append(blocks, b)
		}
	}

	// Remaining blocks are new ones and we have to fetch the meta.json for each of them, in order
	// to find out if their upload has been completed (meta.json is uploaded last) and get the block
	// information to store in the bucket index.
	for id := range discovered {
		b, err := w.updateBlockIndexEntry(ctx, id)
		if err == nil {
			blocks = append(blocks, b)
			continue
		}

		if errors.Is(err, ErrBlockMetaNotFound) {
			partials[id] = err
			level.Warn(w.logger).Log("msg", "skipped partial block when updating bucket index", "block", id.String())
			continue
		}
		if errors.Is(err, errBlockMetaKeyAccessDeniedErr) {
			partials[id] = err
			level.Warn(w.logger).Log("msg", "skipped partial block when updating bucket index due key permission", "block", id.String())
			continue
		}
		if errors.Is(err, ErrBlockMetaCorrupted) {
			partials[id] = err
			level.Error(w.logger).Log("msg", "skipped block with corrupted meta.json when updating bucket index", "block", id.String(), "err", err)
			continue
		}
		return nil, nil, err
	}

	return blocks, partials, nil
}

func (w *Updater) updateBlockIndexEntry(ctx context.Context, id ulid.ULID) (*Block, error) {
	metaFile := path.Join(id.String(), block.MetaFilename)

	// Get the block's meta.json file.
	r, err := w.bkt.ReaderWithExpectedErrs(tsdb.IsOneOfTheExpectedErrors(w.bkt.IsObjNotFoundErr, w.bkt.IsAccessDeniedErr)).Get(ctx, metaFile)
	if w.bkt.IsObjNotFoundErr(err) {
		return nil, ErrBlockMetaNotFound
	}
	if w.bkt.IsAccessDeniedErr(err) {
		return nil, errBlockMetaKeyAccessDeniedErr
	}
	if err != nil {
		return nil, errors.Wrapf(err, "get block meta file: %v", metaFile)
	}
	defer runutil.CloseWithLogOnErr(w.logger, r, "close get block meta file")

	metaContent, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read block meta file: %v", metaFile)
	}

	// Unmarshal it.
	m := metadata.Meta{}
	if err := json.Unmarshal(metaContent, &m); err != nil {
		return nil, errors.Wrapf(ErrBlockMetaCorrupted, "unmarshal block meta file %s: %v", metaFile, err)
	}

	if m.Version != metadata.TSDBVersion1 {
		return nil, errors.Errorf("unexpected block meta version: %s version: %d", metaFile, m.Version)
	}

	block := BlockFromThanosMeta(m)

	// Get the meta.json attributes.
	attrs, err := w.bkt.Attributes(ctx, metaFile)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta file attributes: %v", metaFile)
	}

	// Since the meta.json file is the last file of a block being uploaded and it's immutable
	// we can safely assume that the last modified timestamp of the meta.json is the time when
	// the block has completed to be uploaded.
	block.UploadedAt = attrs.LastModified.Unix()

	return block, nil
}

func (w *Updater) updateParquetBlockIndexEntry(ctx context.Context, id ulid.ULID, block *Block) error {
	marker, err := parquet.ReadConverterMark(ctx, id, w.bkt, w.logger)
	if err != nil {
		return errors.Wrapf(err, "read parquet converter marker file: %v", path.Join(id.String(), parquet.ConverterMarkerFileName))
	}
	// Could be not found or access denied.
	// Just treat it as no parquet block available.
	if marker == nil || marker.Version == 0 {
		return nil
	}

	block.Parquet = &parquet.ConverterMarkMeta{
		Version: marker.Version,
	}
	return nil
}

func (w *Updater) updateBlockMarks(ctx context.Context, old []*BlockDeletionMark) ([]*BlockDeletionMark, map[ulid.ULID]struct{}, int64, error) {
	out := make([]*BlockDeletionMark, 0, len(old))
	deletedBlocks := map[ulid.ULID]struct{}{}
	discovered := map[ulid.ULID]struct{}{}
	totalBlocksBlocksMarkedForNoCompaction := int64(0)

	// Find all markers in the storage.
	err := w.bkt.Iter(ctx, MarkersPathname+"/", func(name string) error {
		if blockID, ok := IsBlockDeletionMarkFilename(path.Base(name)); ok {
			discovered[blockID] = struct{}{}
		}

		if _, ok := IsBlockNoCompactMarkFilename(path.Base(name)); ok {
			totalBlocksBlocksMarkedForNoCompaction++
		}

		return nil
	})
	if err != nil {
		return nil, nil, totalBlocksBlocksMarkedForNoCompaction, errors.Wrap(err, "list block deletion marks")
	}

	// Since deletion marks are immutable, all markers already existing in the index can just be copied.
	for _, m := range old {
		if _, ok := discovered[m.ID]; ok {
			out = append(out, m)
			delete(discovered, m.ID)
		} else {
			deletedBlocks[m.ID] = struct{}{}
		}
	}

	// Remaining markers are new ones and we have to fetch them.
	for id := range discovered {
		m, err := w.updateBlockDeletionMarkIndexEntry(ctx, id)
		if errors.Is(err, ErrBlockDeletionMarkNotFound) {
			// This could happen if the block is permanently deleted between the "list objects" and now.
			level.Warn(w.logger).Log("msg", "skipped missing block deletion mark when updating bucket index", "block", id.String())
			continue
		}
		if errors.Is(err, ErrBlockDeletionMarkCorrupted) {
			level.Error(w.logger).Log("msg", "skipped corrupted block deletion mark when updating bucket index", "block", id.String(), "err", err)
			continue
		}
		if err != nil {
			return nil, nil, totalBlocksBlocksMarkedForNoCompaction, err
		}

		out = append(out, m)
	}

	return out, deletedBlocks, totalBlocksBlocksMarkedForNoCompaction, nil
}

func (w *Updater) updateBlockDeletionMarkIndexEntry(ctx context.Context, id ulid.ULID) (*BlockDeletionMark, error) {
	m := metadata.DeletionMark{}

	if err := metadata.ReadMarker(ctx, w.logger, w.bkt, id.String(), &m); err != nil {
		if errors.Is(err, metadata.ErrorMarkerNotFound) {
			return nil, errors.Wrap(ErrBlockDeletionMarkNotFound, err.Error())
		}
		if errors.Is(err, metadata.ErrorUnmarshalMarker) {
			return nil, errors.Wrap(ErrBlockDeletionMarkCorrupted, err.Error())
		}
		return nil, err
	}

	return BlockDeletionMarkFromThanosMarker(&m), nil
}

func (w *Updater) updateParquetBlocks(ctx context.Context, blocks []*Block) error {
	discoveredParquetBlocks := map[ulid.ULID]struct{}{}

	// Find all parquet markers in the storage.
	if err := w.bkt.Iter(ctx, parquet.ConverterMarkerPrefix+"/", func(name string) error {
		if blockID, ok := IsBlockParquetConverterMarkFilename(path.Base(name)); ok {
			discoveredParquetBlocks[blockID] = struct{}{}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "list block parquet converter marks")
	}

	// Check if parquet mark has been uploaded or deleted for the block.
	for _, m := range blocks {
		if _, ok := discoveredParquetBlocks[m.ID]; ok {
			if err := w.updateParquetBlockIndexEntry(ctx, m.ID, m); err != nil {
				return err
			}
		} else if m.Parquet != nil {
			// Converter marker removed. Reset parquet field.
			m.Parquet = nil
		}
	}
	return nil
}
