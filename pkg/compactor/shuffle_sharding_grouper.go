package compactor

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/ring"
)

type ShuffleShardingGrouper struct {
	ctx                      context.Context
	logger                   log.Logger
	bkt                      objstore.InstrumentedBucket
	acceptMalformedIndex     bool
	enableVerticalCompaction bool
	blocksMarkedForNoCompact prometheus.Counter
	syncerMetrics            *compact.SyncerMetrics
	compactorMetrics         *compactorMetrics
	hashFunc                 metadata.HashFunc
	compactorCfg             Config
	limits                   Limits
	userID                   string
	blockFilesConcurrency    int
	blocksFetchConcurrency   int
	compactionConcurrency    int

	ring               ring.ReadRing
	ringLifecyclerAddr string
	ringLifecyclerID   string

	blockVisitMarkerTimeout     time.Duration
	blockVisitMarkerReadFailed  prometheus.Counter
	blockVisitMarkerWriteFailed prometheus.Counter

	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark
}

func NewShuffleShardingGrouper(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.InstrumentedBucket,
	acceptMalformedIndex bool,
	enableVerticalCompaction bool,
	blocksMarkedForNoCompact prometheus.Counter,
	hashFunc metadata.HashFunc,
	syncerMetrics *compact.SyncerMetrics,
	compactorMetrics *compactorMetrics,
	compactorCfg Config,
	ring ring.ReadRing,
	ringLifecyclerAddr string,
	ringLifecyclerID string,
	limits Limits,
	userID string,
	blockFilesConcurrency int,
	blocksFetchConcurrency int,
	compactionConcurrency int,
	blockVisitMarkerTimeout time.Duration,
	blockVisitMarkerReadFailed prometheus.Counter,
	blockVisitMarkerWriteFailed prometheus.Counter,
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark,
) *ShuffleShardingGrouper {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &ShuffleShardingGrouper{
		ctx:                         ctx,
		logger:                      logger,
		bkt:                         bkt,
		acceptMalformedIndex:        acceptMalformedIndex,
		enableVerticalCompaction:    enableVerticalCompaction,
		blocksMarkedForNoCompact:    blocksMarkedForNoCompact,
		hashFunc:                    hashFunc,
		syncerMetrics:               syncerMetrics,
		compactorMetrics:            compactorMetrics,
		compactorCfg:                compactorCfg,
		ring:                        ring,
		ringLifecyclerAddr:          ringLifecyclerAddr,
		ringLifecyclerID:            ringLifecyclerID,
		limits:                      limits,
		userID:                      userID,
		blockFilesConcurrency:       blockFilesConcurrency,
		blocksFetchConcurrency:      blocksFetchConcurrency,
		compactionConcurrency:       compactionConcurrency,
		blockVisitMarkerTimeout:     blockVisitMarkerTimeout,
		blockVisitMarkerReadFailed:  blockVisitMarkerReadFailed,
		blockVisitMarkerWriteFailed: blockVisitMarkerWriteFailed,
		noCompBlocksFunc:            noCompBlocksFunc,
	}
}

// Groups function modified from https://github.com/cortexproject/cortex/pull/2616
func (g *ShuffleShardingGrouper) Groups(blocks map[ulid.ULID]*metadata.Meta) (res []*compact.Group, err error) {
	noCompactMarked := g.noCompBlocksFunc()
	// First of all we have to group blocks using the Thanos default
	// grouping (based on downsample resolution + external labels).
	mainGroups := map[string][]*metadata.Meta{}
	for _, b := range blocks {
		if _, excluded := noCompactMarked[b.ULID]; !excluded {
			key := b.Thanos.GroupKey()
			mainGroups[key] = append(mainGroups[key], b)
		}
	}

	// For each group, we have to further split it into set of blocks
	// which we can parallelly compact.
	var outGroups []*compact.Group

	// Check if this compactor is on the subring.
	// If the compactor is not on the subring when using the userID as a identifier
	// no plans generated below will be owned by the compactor so we can just return an empty array
	// as there will be no planned groups
	onSubring, err := g.checkSubringForCompactor()
	if err != nil {
		return nil, errors.Wrap(err, "unable to check sub-ring for compactor ownership")
	}
	if !onSubring {
		level.Debug(g.logger).Log("msg", "compactor is not on the current sub-ring skipping user", "user", g.userID)
		return outGroups, nil
	}
	// Metrics for the remaining planned compactions
	var remainingCompactions = 0.
	defer func() {
		g.compactorMetrics.remainingPlannedCompactions.WithLabelValues(g.userID).Set(remainingCompactions)
	}()

	var groups []blocksGroup
	for _, mainBlocks := range mainGroups {
		groups = append(groups, groupBlocksByCompactableRanges(mainBlocks, g.compactorCfg.BlockRanges.ToMilliseconds())...)
	}

	// Ensure groups are sorted by smallest range, oldest min time first. The rationale
	// is that we want to favor smaller ranges first (ie. to deduplicate samples sooner
	// than later) and older ones are more likely to be "complete" (no missing block still
	// to be uploaded).
	sort.SliceStable(groups, func(i, j int) bool {
		iGroup := groups[i]
		jGroup := groups[j]
		iMinTime := iGroup.minTime()
		iMaxTime := iGroup.maxTime()
		jMinTime := jGroup.minTime()
		jMaxTime := jGroup.maxTime()
		iLength := iMaxTime - iMinTime
		jLength := jMaxTime - jMinTime

		if iLength != jLength {
			return iLength < jLength
		}
		if iMinTime != jMinTime {
			return iMinTime < jMinTime
		}

		iGroupHash := hashGroup(g.userID, iGroup.rangeStart, iGroup.rangeEnd)
		iGroupKey := createGroupKey(iGroupHash, iGroup)
		jGroupHash := hashGroup(g.userID, jGroup.rangeStart, jGroup.rangeEnd)
		jGroupKey := createGroupKey(jGroupHash, jGroup)
		// Guarantee stable sort for tests.
		return iGroupKey < jGroupKey
	})

mainLoop:
	for _, group := range groups {
		var blockIds []string
		for _, block := range group.blocks {
			blockIds = append(blockIds, block.ULID.String())
		}
		blocksInfo := strings.Join(blockIds, ",")
		level.Info(g.logger).Log("msg", "check group", "blocks", blocksInfo)

		// Nothing to do if we don't have at least 2 blocks.
		if len(group.blocks) < 2 {
			continue
		}

		groupHash := hashGroup(g.userID, group.rangeStart, group.rangeEnd)

		if isVisited, err := g.isGroupVisited(group.blocks, g.ringLifecyclerID); err != nil {
			level.Warn(g.logger).Log("msg", "unable to check if blocks in group are visited", "group hash", groupHash, "err", err, "group", group.String())
			continue
		} else if isVisited {
			level.Info(g.logger).Log("msg", "skipping group because at least one block in group is visited", "group_hash", groupHash)
			continue
		}

		remainingCompactions++
		groupKey := createGroupKey(groupHash, group)

		level.Info(g.logger).Log("msg", "found compactable group for user", "group_hash", groupHash, "group", group.String())
		blockVisitMarker := BlockVisitMarker{
			VisitTime:   time.Now().Unix(),
			CompactorID: g.ringLifecyclerID,
			Version:     VisitMarkerVersion1,
		}
		markBlocksVisited(g.ctx, g.bkt, g.logger, group.blocks, blockVisitMarker, g.blockVisitMarkerWriteFailed)

		// All the blocks within the same group have the same downsample
		// resolution and external labels.
		resolution := group.blocks[0].Thanos.Downsample.Resolution
		externalLabels := labels.FromMap(group.blocks[0].Thanos.Labels)
		timeRange := group.rangeEnd - group.rangeStart
		metricLabelValues := []string{
			g.userID,
			fmt.Sprintf("%d", timeRange),
		}
		thanosGroup, err := compact.NewGroup(
			log.With(g.logger, "groupKey", groupKey, "rangeStart", group.rangeStartTime().String(), "rangeEnd", group.rangeEndTime().String(), "externalLabels", externalLabels, "downsampleResolution", resolution),
			g.bkt,
			groupKey,
			externalLabels,
			resolution,
			g.acceptMalformedIndex,
			true, // Enable vertical compaction.
			g.compactorMetrics.compactions.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.compactionRunsStarted.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.compactionRunsCompleted.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.compactionFailures.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.verticalCompactions.WithLabelValues(metricLabelValues...),
			g.syncerMetrics.GarbageCollectedBlocks,
			g.syncerMetrics.BlocksMarkedForDeletion,
			g.blocksMarkedForNoCompact,
			g.hashFunc,
			g.blockFilesConcurrency,
			g.blocksFetchConcurrency,
		)
		if err != nil {
			return nil, errors.Wrap(err, "create compaction group")
		}

		for _, m := range group.blocks {
			if err := thanosGroup.AppendMeta(m); err != nil {
				return nil, errors.Wrap(err, "add block to compaction group")
			}
		}

		outGroups = append(outGroups, thanosGroup)
		if len(outGroups) == g.compactionConcurrency {
			break mainLoop
		}
	}

	level.Info(g.logger).Log("msg", fmt.Sprintf("total groups for compaction: %d", len(outGroups)))

	return outGroups, nil
}

func (g *ShuffleShardingGrouper) isGroupVisited(blocks []*metadata.Meta, compactorID string) (bool, error) {
	for _, block := range blocks {
		blockID := block.ULID.String()
		blockVisitMarker, err := ReadBlockVisitMarker(g.ctx, g.bkt, g.logger, blockID, g.blockVisitMarkerReadFailed)
		if err != nil {
			if errors.Is(err, ErrorBlockVisitMarkerNotFound) {
				level.Debug(g.logger).Log("msg", "no visit marker file for block", "blockID", blockID)
				continue
			}
			level.Error(g.logger).Log("msg", "unable to read block visit marker file", "blockID", blockID, "err", err)
			return true, err
		}
		if compactorID != blockVisitMarker.CompactorID && blockVisitMarker.isVisited(g.blockVisitMarkerTimeout) {
			level.Debug(g.logger).Log("msg", fmt.Sprintf("visited block: %s", blockID))
			return true, nil
		}
	}
	return false, nil
}

// Check whether this compactor exists on the subring based on user ID
func (g *ShuffleShardingGrouper) checkSubringForCompactor() (bool, error) {
	subRing := g.ring.ShuffleShard(g.userID, g.limits.CompactorTenantShardSize(g.userID))

	rs, err := subRing.GetAllHealthy(RingOp)
	if err != nil {
		return false, err
	}

	return rs.Includes(g.ringLifecyclerAddr), nil
}

// hashGroup Get the hash of a group based on the UserID, and the starting and ending time of the group's range.
func hashGroup(userID string, rangeStart int64, rangeEnd int64) uint32 {
	groupString := fmt.Sprintf("%v%v%v", userID, rangeStart, rangeEnd)

	return hashString(groupString)
}

func hashString(s string) uint32 {
	hasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = hasher.Write([]byte(s))
	result := hasher.Sum32()

	return result
}

func createGroupKey(groupHash uint32, group blocksGroup) string {
	return fmt.Sprintf("%v%s", groupHash, group.blocks[0].Thanos.GroupKey())
}

// blocksGroup struct and functions copied and adjusted from https://github.com/cortexproject/cortex/pull/2616
type blocksGroup struct {
	rangeStart int64 // Included.
	rangeEnd   int64 // Excluded.
	blocks     []*metadata.Meta
	key        string
}

// overlaps returns whether the group range overlaps with the input group.
func (g blocksGroup) overlaps(other blocksGroup) bool {
	if g.rangeStart >= other.rangeEnd || other.rangeStart >= g.rangeEnd {
		return false
	}

	return true
}

func (g blocksGroup) rangeStartTime() time.Time {
	return time.Unix(0, g.rangeStart*int64(time.Millisecond)).UTC()
}

func (g blocksGroup) rangeEndTime() time.Time {
	return time.Unix(0, g.rangeEnd*int64(time.Millisecond)).UTC()
}

func (g blocksGroup) String() string {
	out := strings.Builder{}
	out.WriteString(fmt.Sprintf("Group range start: %d, range end: %d, key %v, blocks: ", g.rangeStart, g.rangeEnd, g.key))

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

		// If the group's maxTime is after 1 block range, we can compact assuming that
		// all the required blocks have already been uploaded.
		if int64(ulid.Now()) > group.maxTime()+group.rangeLength() {
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

		// Skip blocks that don't fall into the range. This can happen via misalignment or
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
	}

	return tr * ((m.MinTime - tr + 1) / tr)
}

func sortMetasByMinTime(metas []*metadata.Meta) {
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	})
}
