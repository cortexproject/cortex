package compactor

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type TimeShardingCompactor struct {
	tsdbCompactor tsdb.Compactor
	ranges        []int64
}

func NewTimeShardingCompactor(tsdbCompactor tsdb.Compactor, ranges []int64) *TimeShardingCompactor {
	return &TimeShardingCompactor{
		tsdbCompactor: tsdbCompactor,
		ranges:        ranges,
	}
}

// Plan implements tsdb.Compactor
func (c *TimeShardingCompactor) Plan(dir string) ([]string, error) {
	// Given the TSDB compactor runs against a temporary directory
	// containing only the set of blocks we wanna compact within a
	// single range, all blocks there should be returned sorted by
	// MinTime.
	blocks, err := scanBlocksFromDir(dir)
	if err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, nil
	}

	sortMetasByMinTime(blocks)

	// Ensure all blocks fits within the largest range. This is a double check
	// to ensure there's no bug in the previous blocks grouping, given this Plan()
	// is just a pass-through.
	largestRange := c.ranges[len(c.ranges)-1]
	rangeStart := getRangeStart(blocks[0], largestRange)
	rangeEnd := rangeStart + largestRange

	for _, b := range blocks {
		if b.MinTime < rangeStart || b.MaxTime > rangeEnd {
			return nil, fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", b.ULID.String(), b.MinTime, b.MaxTime, rangeStart, rangeEnd)
		}
	}

	// Build the dirs to return, keeping the MinTime sorting.
	sortedDirs := make([]string, 0, len(blocks))
	for _, block := range blocks {
		sortedDirs = append(sortedDirs, filepath.Join(dir, block.ULID.String()))
	}

	return sortedDirs, nil
}

// Write implements tsdb.Compactor
func (c *TimeShardingCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, parent *tsdb.BlockMeta) (ulid.ULID, error) {
	return c.tsdbCompactor.Write(dest, b, mint, maxt, parent)
}

// Compact implements tsdb.Compactor
func (c *TimeShardingCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) (ulid.ULID, error) {
	return c.tsdbCompactor.Compact(dest, dirs, open)
}

func scanBlocksFromDir(dir string) ([]*metadata.Meta, error) {
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var blocks []*metadata.Meta

	for _, fi := range entries {
		if !isBlockDir(fi) {
			continue
		}

		// Parse the metadata file.
		blockDir := filepath.Join(dir, fi.Name())
		meta, err := metadata.Read(blockDir)
		if err != nil {
			return nil, errors.Wrapf(err, "read block meta at %s", blockDir)
		}

		blocks = append(blocks, meta)
	}

	return blocks, nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}
