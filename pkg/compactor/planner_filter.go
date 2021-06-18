package compactor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type PlannerFilter struct {
	userID         string
	ulogger        log.Logger
	bucket         objstore.InstrumentedBucket
	fetcherFilters []block.MetadataFilter
	compactorCfg   Config
	metaSyncDir    string

	plans []*blocksGroup
}

func NewPlannerFilter(ctx context.Context, userID string, ulogger log.Logger, bucket objstore.InstrumentedBucket, fetcherFilters []block.MetadataFilter, compactorCfg Config, metaSyncDir string) (*PlannerFilter, error) {
	f := &PlannerFilter{
		userID:         userID,
		ulogger:        ulogger,
		bucket:         bucket,
		fetcherFilters: fetcherFilters,
		compactorCfg:   compactorCfg,
		metaSyncDir:    metaSyncDir,
	}

	return f, nil
}

// Gets the blocks of the user.
func (f *PlannerFilter) getUserBlocks(ctx context.Context) (map[ulid.ULID]*metadata.Meta, error) {
	fetcher, err := block.NewMetaFetcher(
		f.ulogger,
		f.compactorCfg.MetaSyncConcurrency,
		f.bucket,
		f.metaSyncDir,
		prometheus.NewRegistry(),
		// List of filters to apply (order matters).
		f.fetcherFilters,
		nil,
	)
	if err != nil {
		return nil, err
	}

	metas, _, err := fetcher.Fetch(ctx)
	if err != nil {
		return nil, err
	}

	return metas, nil
}

// Fetches blocks and generates plans that can be parallized and saves them
func (f *PlannerFilter) fetchBlocksAndGeneratePlans(ctx context.Context) error {
	// Get blocks
	blocks, err := f.getUserBlocks(ctx)
	if err != nil {
		return err
	}

	return f.generatePlans(ctx, blocks)
}

// Generates plans that can be parallized and saves them
func (f *PlannerFilter) generatePlans(ctx context.Context, blocks map[ulid.ULID]*metadata.Meta) error {
	// First of all we have to group blocks using the Thanos default
	// grouping (based on downsample resolution + external labels).
	mainGroups := map[string][]*metadata.Meta{}
	for _, b := range blocks {
		key := compact.DefaultGroupKey(b.Thanos)
		mainGroups[key] = append(mainGroups[key], b)
	}

	var plans []*blocksGroup

	for _, mainBlocks := range mainGroups {
		for i, plan := range groupBlocksByCompactableRanges(mainBlocks, f.compactorCfg.BlockRanges.ToMilliseconds()) {
			// Nothing to do if we don't have at least 2 blocks.
			if len(plan.blocks) < 2 {
				continue
			}

			plan.key = i

			level.Info(f.ulogger).Log("msg", "Found plan for user", "user", f.userID, "plan", plan.String())

			plans = append(plans, &plan)
		}
	}

	// Ensure groups are sorted by smallest range, oldest min time first. The rationale
	// is that we wanna favor smaller ranges first (ie. to deduplicate samples sooner
	// than later) and older ones are more likely to be "complete" (no missing block still
	// to be uploaded).
	sort.SliceStable(plans, func(i, j int) bool {
		iLength := plans[i].maxTime() - plans[i].minTime()
		jLength := plans[j].maxTime() - plans[j].minTime()

		if iLength != jLength {
			return iLength < jLength
		}
		if plans[i].minTime() != plans[j].minTime() {
			return plans[i].minTime() < plans[j].minTime()
		}

		// Guarantee stable sort for tests.
		return plans[i].key < plans[j].key
	})

	f.plans = plans

	return nil
}

// Filter removes the blocks for every single plan except one.
// Currently we are just using the first plan every single time.
// TODO: Filter plans by putting each plan on the ring and having the compactor select a plan (filter the rest)
func (f *PlannerFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, _ *extprom.TxGaugeVec) error {
	// Plans have to exist to be filtered, if no blocks need to be compacted then nothing needs to be filtered.
	if len(f.plans) < 1 {
		return nil
	}

	// Delete blocks for each plan except the first one.
	for _, p := range f.plans[1:] {
		for _, b := range p.blocks {
			delete(metas, b.BlockMeta.ULID) // check what happens if it tries to delete a key that doesn't exist
		}
	}

	return nil
}

// blocksGroup struct and functions copied and adjusted from https://github.com/cortexproject/cortex/pull/2616
type blocksGroup struct {
	rangeStart int64 // Included.
	rangeEnd   int64 // Excluded.
	blocks     []*metadata.Meta
	key        int
}

// overlaps returns whether the group range overlaps with the input group.
func (g blocksGroup) overlaps(other blocksGroup) bool {
	if g.rangeStart >= other.rangeEnd || other.rangeStart >= g.rangeEnd {
		return false
	}

	return true
}

func (g blocksGroup) String() string {
	out := strings.Builder{}
	out.WriteString(fmt.Sprintf("Group range start: %d, range end: %d, blocks: ", g.rangeStart, g.rangeEnd))

	for i, b := range g.blocks {
		if i > 0 {
			out.WriteString(", ")
		}

		minT := time.Unix(0, b.MinTime*int64(time.Millisecond)).UTC()
		maxT := time.Unix(0, b.MaxTime*int64(time.Millisecond)).UTC()
		out.WriteString(fmt.Sprintf("%s (min time: %s, max time: %s)", b.ULID.String(), minT.String(), maxT.String()))
	}

	return out.String()
}

func (g blocksGroup) rangeLength() int64 {
	return g.rangeEnd - g.rangeStart
}

// minTime returns the MinTime across all blocks in the group.
func (g blocksGroup) minTime() int64 {
	// Blocks are expected to be sorted by MinTime.
	return g.blocks[0].MinTime
}

// maxTime returns the MaxTime across all blocks in the group.
func (g blocksGroup) maxTime() int64 {
	max := g.blocks[0].MaxTime

	for _, b := range g.blocks[1:] {
		if b.MaxTime > max {
			max = b.MaxTime
		}
	}

	return max
}

// groupBlocksByCompactableRanges groups input blocks by compactable ranges, giving preference
// to smaller ranges. If a smaller range contains more than 1 block (and thus it should
// be compacted), the larger range block group is not generated until each of its
// smaller ranges have 1 block each at most.
func groupBlocksByCompactableRanges(blocks []*metadata.Meta, ranges []int64) []blocksGroup {
	if len(blocks) == 0 {
		return nil
	}

	// Sort blocks by min time.
	sortMetasByMinTime(blocks)

	var groups []blocksGroup

	for _, tr := range ranges {
	nextGroup:
		for _, group := range groupBlocksByRange(blocks, tr) {
			// Exclude groups with a single block, because no compaction is required.
			if len(group.blocks) < 2 {
				continue
			}

			// Ensure this group's range does not overlap with any group already scheduled
			// for compaction by a smaller range, because we need to guarantee that smaller ranges
			// are compacted first.
			for _, c := range groups {
				if group.overlaps(c) {
					continue nextGroup
				}
			}

			groups = append(groups, group)
		}
	}

	// Ensure we don't compact the most recent blocks prematurely when another one of
	// the same size still fits in the range. To do it, we consider valid a group only
	// if it's before the most recent block or if it fully covers the range.
	highestMinTime := blocks[len(blocks)-1].MinTime

	for idx := 0; idx < len(groups); {
		group := groups[idx]

		// If the group covers a range before the most recent block, it's fine.
		if group.rangeEnd <= highestMinTime {
			idx++
			continue
		}

		// If the group covers the full range, it's fine.
		if group.maxTime()-group.minTime() == group.rangeLength() {
			idx++
			continue
		}

		// We hit into a group which would compact recent blocks prematurely,
		// so we need to filter it out.
		groups = append(groups[:idx], groups[idx+1:]...)
	}

	return groups
}

// groupBlocksByRange splits the blocks by the time range. The range sequence starts at 0.
// Input blocks are expected to be sorted by MinTime.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func groupBlocksByRange(blocks []*metadata.Meta, tr int64) []blocksGroup {
	var ret []blocksGroup

	for i := 0; i < len(blocks); {
		var (
			group blocksGroup
			m     = blocks[i]
		)

		group.rangeStart = getRangeStart(m, tr)
		group.rangeEnd = group.rangeStart + tr

		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > group.rangeEnd {
			i++
			continue
		}

		// Add all blocks to the current group that are within [t0, t0+tr].
		for ; i < len(blocks); i++ {
			// If the block does not start within this group, then we should break the iteration
			// and move it to the next group.
			if blocks[i].MinTime >= group.rangeEnd {
				break
			}

			// If the block doesn't fall into this group, but it started within this group then it
			// means it spans across multiple ranges and we should skip it.
			if blocks[i].MaxTime > group.rangeEnd {
				continue
			}

			group.blocks = append(group.blocks, blocks[i])
		}

		if len(group.blocks) > 0 {
			ret = append(ret, group)
		}
	}

	return ret
}

func getRangeStart(m *metadata.Meta, tr int64) int64 {
	// Compute start of aligned time range of size tr closest to the current block's start.
	// This code has been copied from TSDB.
	if m.MinTime >= 0 {
		return tr * (m.MinTime / tr)
	} else {
		return tr * ((m.MinTime - tr + 1) / tr)
	}
}

func sortMetasByMinTime(metas []*metadata.Meta) {
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	})
}
