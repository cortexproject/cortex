package compactor

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/ring"
)

type ShuffleShardingGrouper struct {
	ctx                         context.Context
	logger                      log.Logger
	bkt                         objstore.InstrumentedBucket
	acceptMalformedIndex        bool
	enableVerticalCompaction    bool
	reg                         prometheus.Registerer
	blocksMarkedForDeletion     prometheus.Counter
	blocksMarkedForNoCompact    prometheus.Counter
	garbageCollectedBlocks      prometheus.Counter
	remainingPlannedCompactions prometheus.Gauge
	hashFunc                    metadata.HashFunc
	compactions                 *prometheus.CounterVec
	compactionRunsStarted       *prometheus.CounterVec
	compactionRunsCompleted     *prometheus.CounterVec
	compactionFailures          *prometheus.CounterVec
	verticalCompactions         *prometheus.CounterVec
	compactorCfg                Config
	limits                      Limits
	userID                      string
	blockFilesConcurrency       int
	blocksFetchConcurrency      int
	compactionConcurrency       int

	ring               ring.ReadRing
	ringLifecyclerAddr string
	ringLifecyclerID   string

	noCompBlocksFunc                func() map[ulid.ULID]*metadata.NoCompactMark
	blockVisitMarkerTimeout         time.Duration
	blockVisitMarkerReadFailed      prometheus.Counter
	blockVisitMarkerWriteFailed     prometheus.Counter
	partitionedGroupInfoReadFailed  prometheus.Counter
	partitionedGroupInfoWriteFailed prometheus.Counter
}

func NewShuffleShardingGrouper(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.InstrumentedBucket,
	acceptMalformedIndex bool,
	enableVerticalCompaction bool,
	reg prometheus.Registerer,
	blocksMarkedForDeletion prometheus.Counter,
	blocksMarkedForNoCompact prometheus.Counter,
	garbageCollectedBlocks prometheus.Counter,
	remainingPlannedCompactions prometheus.Gauge,
	hashFunc metadata.HashFunc,
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
	partitionedGroupInfoReadFailed prometheus.Counter,
	partitionedGroupInfoWriteFailed prometheus.Counter,
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
		reg:                         reg,
		blocksMarkedForDeletion:     blocksMarkedForDeletion,
		blocksMarkedForNoCompact:    blocksMarkedForNoCompact,
		garbageCollectedBlocks:      garbageCollectedBlocks,
		remainingPlannedCompactions: remainingPlannedCompactions,
		hashFunc:                    hashFunc,
		// Metrics are copied from Thanos DefaultGrouper constructor
		compactions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_group_compactions_total",
			Help: "Total number of group compaction attempts that resulted in a new block.",
		}, []string{"group"}),
		compactionRunsStarted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_group_compaction_runs_started_total",
			Help: "Total number of group compaction attempts.",
		}, []string{"group"}),
		compactionRunsCompleted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_group_compaction_runs_completed_total",
			Help: "Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.",
		}, []string{"group"}),
		compactionFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_group_compactions_failures_total",
			Help: "Total number of failed group compactions.",
		}, []string{"group"}),
		verticalCompactions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_group_vertical_compactions_total",
			Help: "Total number of group compaction attempts that resulted in a new block based on overlapping blocks.",
		}, []string{"group"}),
		compactorCfg:                    compactorCfg,
		ring:                            ring,
		ringLifecyclerAddr:              ringLifecyclerAddr,
		ringLifecyclerID:                ringLifecyclerID,
		limits:                          limits,
		userID:                          userID,
		blockFilesConcurrency:           blockFilesConcurrency,
		blocksFetchConcurrency:          blocksFetchConcurrency,
		compactionConcurrency:           compactionConcurrency,
		blockVisitMarkerTimeout:         blockVisitMarkerTimeout,
		blockVisitMarkerReadFailed:      blockVisitMarkerReadFailed,
		blockVisitMarkerWriteFailed:     blockVisitMarkerWriteFailed,
		partitionedGroupInfoReadFailed:  partitionedGroupInfoReadFailed,
		partitionedGroupInfoWriteFailed: partitionedGroupInfoWriteFailed,
		noCompBlocksFunc:                noCompBlocksFunc,
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
	defer func() { g.remainingPlannedCompactions.Set(remainingCompactions) }()

	var groups []blocksGroup
	for _, mainBlocks := range mainGroups {
		groups = append(groups, groupBlocksByCompactableRanges(mainBlocks, g.compactorCfg.BlockRanges.ToMilliseconds())...)
	}

	// Ensure groups are sorted by smallest range, oldest min time first. The rationale
	// is that we wanna favor smaller ranges first (ie. to deduplicate samples sooner
	// than later) and older ones are more likely to be "complete" (no missing block still
	// to be uploaded).
	sort.SliceStable(groups, func(i, j int) bool {
		iGroup := groups[i]
		jGroup := groups[j]
		iRangeStart := iGroup.rangeStart
		iRangeEnd := iGroup.rangeEnd
		jRangeStart := jGroup.rangeStart
		jRangeEnd := jGroup.rangeEnd
		iLength := iRangeEnd - iRangeStart
		jLength := jRangeEnd - jRangeStart

		if iLength != jLength {
			return iLength < jLength
		}
		if iRangeStart != jRangeStart {
			return iRangeStart < jRangeStart
		}

		iGroupHash := hashGroup(g.userID, iRangeStart, iRangeEnd)
		iGroupKey := createGroupKey(iGroupHash, iGroup)
		jGroupHash := hashGroup(g.userID, jRangeStart, jRangeEnd)
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

		partitionedGroupInfo, err := g.generatePartitionBlockGroup(group, groupHash)
		if err != nil {
			level.Warn(g.logger).Log("msg", "unable to update partitioned group info", "partitioned_group_id", groupHash, "err", err)
			continue
		}
		level.Debug(g.logger).Log("msg", "generated partitioned groups", "groups", partitionedGroupInfo)

		partitionedGroupID := partitionedGroupInfo.PartitionedGroupID
		partitionCount := partitionedGroupInfo.PartitionCount
		for _, partition := range partitionedGroupInfo.Partitions {
			partitionID := partition.PartitionID
			partitionedGroup, err := createBlocksGroup(blocks, partition.Blocks, partitionedGroupInfo.RangeStart, partitionedGroupInfo.RangeEnd)
			if err != nil {
				level.Error(g.logger).Log("msg", "unable to create partitioned group", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "err", err)
				continue
			}
			if isVisited, err := g.isGroupVisited(partitionedGroup.blocks, partitionID, g.ringLifecyclerID); err != nil {
				level.Warn(g.logger).Log("msg", "unable to check if blocks in partition are visited", "group hash", groupHash, "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "err", err, "group", group.String())
				continue
			} else if isVisited {
				level.Info(g.logger).Log("msg", "skipping group because at least one block in partition is visited", "group_hash", groupHash, "partitioned_group_id", partitionedGroupID, "partition_id", partitionID)
				continue
			}

			remainingCompactions++
			partitionedGroupKey := createGroupKeyWithPartitionID(groupHash, partitionID, *partitionedGroup)

			level.Info(g.logger).Log("msg", "found compactable group for user", "group_hash", groupHash, "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "partition_count", partitionCount, "group", partitionedGroup.String())
			blockVisitMarker := BlockVisitMarker{
				VisitTime:          time.Now().Unix(),
				CompactorID:        g.ringLifecyclerID,
				Status:             Pending,
				PartitionedGroupID: partitionedGroupID,
				PartitionID:        partitionID,
				Version:            VisitMarkerVersion1,
			}
			markBlocksVisited(g.ctx, g.bkt, g.logger, partitionedGroup.blocks, blockVisitMarker, g.blockVisitMarkerWriteFailed)

			resolution := partitionedGroup.blocks[0].Thanos.Downsample.Resolution
			externalLabels := labels.FromMap(partitionedGroup.blocks[0].Thanos.Labels)
			thanosGroup, err := compact.NewGroup(
				log.With(g.logger, "groupKey", partitionedGroupKey, "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "partition_count", partitionCount, "rangeStart", partitionedGroup.rangeStartTime().String(), "rangeEnd", partitionedGroup.rangeEndTime().String(), "externalLabels", externalLabels, "downsampleResolution", resolution),
				g.bkt,
				partitionedGroupKey,
				externalLabels,
				resolution,
				g.acceptMalformedIndex,
				true, // Enable vertical compaction.
				g.compactions.WithLabelValues(partitionedGroupKey),
				g.compactionRunsStarted.WithLabelValues(partitionedGroupKey),
				g.compactionRunsCompleted.WithLabelValues(partitionedGroupKey),
				g.compactionFailures.WithLabelValues(partitionedGroupKey),
				g.verticalCompactions.WithLabelValues(partitionedGroupKey),
				g.garbageCollectedBlocks,
				g.blocksMarkedForDeletion,
				g.blocksMarkedForNoCompact,
				g.hashFunc,
				g.blockFilesConcurrency,
				g.blocksFetchConcurrency,
			)
			if err != nil {
				level.Error(g.logger).Log("msg", "failed to create partitioned group", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "partition_count", partitionCount, "blocks", partition.Blocks)
			}

			for _, m := range partitionedGroup.blocks {
				if err := thanosGroup.AppendMeta(m); err != nil {
					level.Error(g.logger).Log("msg", "failed to add block to partitioned group", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "partition_count", partitionCount, "block", m.ULID)
				}
			}
			thanosGroup.SetExtensions(&CortexMetaExtensions{
				PartitionInfo: &PartitionInfo{
					PartitionedGroupID: partitionedGroupID,
					PartitionCount:     partitionCount,
					PartitionID:        partitionID,
				},
			})

			outGroups = append(outGroups, thanosGroup)
			if len(outGroups) >= g.compactionConcurrency {
				break mainLoop
			}
		}
	}

	level.Info(g.logger).Log("msg", fmt.Sprintf("total groups for compaction: %d", len(outGroups)))

	return outGroups, nil
}

func (g *ShuffleShardingGrouper) generatePartitionBlockGroup(group blocksGroup, groupHash uint32) (*PartitionedGroupInfo, error) {
	partitionedGroupInfo, err := g.partitionBlockGroup(group, groupHash)
	if err != nil {
		return nil, err
	}
	updatedPartitionedGroupInfo, err := UpdatePartitionedGroupInfo(g.ctx, g.bkt, g.logger, *partitionedGroupInfo, g.partitionedGroupInfoReadFailed, g.partitionedGroupInfoWriteFailed)
	if err != nil {
		level.Warn(g.logger).Log("msg", "unable to update partitioned group info", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "err", err)
		return nil, err
	}
	level.Debug(g.logger).Log("msg", "generated partitioned groups", "groups", updatedPartitionedGroupInfo)
	return updatedPartitionedGroupInfo, nil
}

func (g *ShuffleShardingGrouper) partitionBlockGroup(group blocksGroup, groupHash uint32) (*PartitionedGroupInfo, error) {
	partitionCount := g.calculatePartitionCount(group)
	blocksByMinTime := g.groupBlocksByMinTime(group)
	partitionedGroups, err := g.partitionBlocksGroup(partitionCount, blocksByMinTime, group.rangeStart, group.rangeEnd)
	if err != nil {
		return nil, err
	}

	var partitions []Partition
	for partitionID, partitionedGroup := range partitionedGroups {
		var blockIDs []ulid.ULID
		for _, m := range partitionedGroup.blocks {
			blockIDs = append(blockIDs, m.ULID)
		}
		partitions = append(partitions, Partition{
			PartitionID: partitionID,
			Blocks:      blockIDs,
		})
	}
	partitionedGroupInfo := PartitionedGroupInfo{
		PartitionedGroupID: groupHash,
		PartitionCount:     partitionCount,
		Partitions:         partitions,
		RangeStart:         group.rangeStart,
		RangeEnd:           group.rangeEnd,
		Version:            PartitionedGroupInfoVersion1,
	}
	return &partitionedGroupInfo, nil
}

func (g *ShuffleShardingGrouper) calculatePartitionCount(group blocksGroup) int {
	indexSizeLimit := g.compactorCfg.PartitionIndexSizeLimitInBytes
	seriesCountLimit := g.compactorCfg.PartitionSeriesCountLimit
	totalIndexSizeInBytes := int64(0)
	totalSeriesCount := int64(0)
	for _, block := range group.blocks {
		blockFiles := block.Thanos.Files
		totalSeriesCount += int64(block.Stats.NumSeries)
		var indexFile *metadata.File
		for _, file := range blockFiles {
			if file.RelPath == thanosblock.IndexFilename {
				indexFile = &file
			}
		}
		if indexFile == nil {
			level.Debug(g.logger).Log("msg", "unable to find index file in metadata", "block", block.ULID)
			break
		}
		indexSize := indexFile.SizeBytes
		totalIndexSizeInBytes += indexSize
	}
	partitionNumberBasedOnIndex := 1
	if indexSizeLimit > 0 && totalIndexSizeInBytes > indexSizeLimit {
		partitionNumberBasedOnIndex = g.findNearestPartitionNumber(float64(totalIndexSizeInBytes), float64(indexSizeLimit))
	}
	partitionNumberBasedOnSeries := 1
	if seriesCountLimit > 0 && totalSeriesCount > seriesCountLimit {
		partitionNumberBasedOnSeries = g.findNearestPartitionNumber(float64(totalSeriesCount), float64(seriesCountLimit))
	}
	partitionNumber := partitionNumberBasedOnIndex
	if partitionNumberBasedOnSeries > partitionNumberBasedOnIndex {
		partitionNumber = partitionNumberBasedOnSeries
	}
	level.Debug(g.logger).Log("msg", "calculated partition number for group", "group", group.String(), "partition_number", partitionNumber, "total_index_size", totalIndexSizeInBytes, "index_size_limit", indexSizeLimit, "total_series_count", totalSeriesCount, "series_count_limit", seriesCountLimit)
	return partitionNumber
}

func (g *ShuffleShardingGrouper) findNearestPartitionNumber(size float64, limit float64) int {
	return int(math.Pow(2, math.Ceil(math.Log2(size/limit))))
}

func (g *ShuffleShardingGrouper) groupBlocksByMinTime(group blocksGroup) map[int64][]*metadata.Meta {
	blocksByMinTime := make(map[int64][]*metadata.Meta)
	for _, block := range group.blocks {
		blockRange := block.MaxTime - block.MinTime
		minTime := block.MinTime
		for _, tr := range g.compactorCfg.BlockRanges.ToMilliseconds() {
			if blockRange <= tr {
				minTime = tr * (block.MinTime / tr)
				break
			}
		}
		blocksByMinTime[minTime] = append(blocksByMinTime[minTime], block)
	}
	return blocksByMinTime
}

func (g *ShuffleShardingGrouper) partitionBlocksGroup(partitionCount int, blocksByMinTime map[int64][]*metadata.Meta, rangeStart int64, rangeEnd int64) (map[int]blocksGroup, error) {
	partitionedGroups := make(map[int]blocksGroup)
	addToPartitionedGroups := func(blocks []*metadata.Meta, partitionID int) {
		if _, ok := partitionedGroups[partitionID]; !ok {
			partitionedGroups[partitionID] = blocksGroup{
				rangeStart: rangeStart,
				rangeEnd:   rangeEnd,
				blocks:     []*metadata.Meta{},
			}
		}
		partitionedGroup := partitionedGroups[partitionID]
		partitionedGroup.blocks = append(partitionedGroup.blocks, blocks...)
		partitionedGroups[partitionID] = partitionedGroup
	}

	for _, blocksInSameTimeInterval := range blocksByMinTime {
		numOfBlocks := len(blocksInSameTimeInterval)
		numBlocksCheck := math.Log2(float64(numOfBlocks))
		if math.Ceil(numBlocksCheck) == math.Floor(numBlocksCheck) {
			// Case that number of blocks in this time interval is 2^n, should
			// use modulo calculation to find blocks for each partition ID.
			for _, block := range blocksInSameTimeInterval {
				partitionInfo, err := GetPartitionInfo(*block)
				if err != nil {
					return nil, err
				}
				if partitionInfo == nil {
					// For legacy blocks with level > 1, treat PartitionID is always 0.
					// So it can be included in every partition.
					partitionInfo = &PartitionInfo{
						PartitionID: 0,
					}
				}
				if numOfBlocks < partitionCount {
					for partitionID := partitionInfo.PartitionID; partitionID < partitionCount; partitionID += numOfBlocks {
						addToPartitionedGroups([]*metadata.Meta{block}, partitionID)
					}
				} else if numOfBlocks == partitionCount {
					addToPartitionedGroups([]*metadata.Meta{block}, partitionInfo.PartitionID)
				} else {
					addToPartitionedGroups([]*metadata.Meta{block}, partitionInfo.PartitionID%partitionCount)
				}
			}
		} else {
			// Case that number of blocks in this time interval is not 2^n, should
			// include all blocks in all partitions.
			for partitionID := 0; partitionID < partitionCount; partitionID++ {
				addToPartitionedGroups(blocksInSameTimeInterval, partitionID)
			}
		}
	}
	return partitionedGroups, nil
}

func (g *ShuffleShardingGrouper) isGroupVisited(blocks []*metadata.Meta, partitionID int, compactorID string) (bool, error) {
	for _, block := range blocks {
		blockID := block.ULID.String()
		blockVisitMarker, err := ReadBlockVisitMarker(g.ctx, g.bkt, g.logger, blockID, partitionID, g.blockVisitMarkerReadFailed)
		if err != nil {
			if errors.Is(err, ErrorBlockVisitMarkerNotFound) {
				level.Warn(g.logger).Log("msg", "no visit marker file for block", "partition_id", partitionID, "block_id", blockID)
				continue
			}
			level.Error(g.logger).Log("msg", "unable to read block visit marker file", "partition_id", partitionID, "block_id", blockID, "err", err)
			return true, err
		}
		if blockVisitMarker.isCompleted() {
			level.Info(g.logger).Log("msg", "block visit marker with partition ID is completed", "partition_id", partitionID, "block_id", blockID)
			return true, nil
		}
		if compactorID != blockVisitMarker.CompactorID && blockVisitMarker.isVisited(g.blockVisitMarkerTimeout, partitionID) {
			level.Info(g.logger).Log("msg", "visited block with partition ID", "partition_id", partitionID, "block_id", blockID)
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

// Get the hash of a group based on the UserID, and the starting and ending time of the group's range.
func hashGroup(userID string, rangeStart int64, rangeEnd int64) uint32 {
	groupString := fmt.Sprintf("%v%v%v", userID, rangeStart, rangeEnd)
	groupHasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = groupHasher.Write([]byte(groupString))
	groupHash := groupHasher.Sum32()

	return groupHash
}

func createGroupKey(groupHash uint32, group blocksGroup) string {
	return fmt.Sprintf("%v%s", groupHash, group.blocks[0].Thanos.GroupKey())
}

func createGroupKeyWithPartitionID(groupHash uint32, partitionID int, group blocksGroup) string {
	return fmt.Sprintf("%v%d%s", groupHash, partitionID, group.blocks[0].Thanos.GroupKey())
}

func createBlocksGroup(blocks map[ulid.ULID]*metadata.Meta, blockIDs []ulid.ULID, rangeStart int64, rangeEnd int64) (*blocksGroup, error) {
	var group blocksGroup
	group.rangeStart = rangeStart
	group.rangeEnd = rangeEnd
	for _, blockID := range blockIDs {
		m, ok := blocks[blockID]
		if !ok {
			return nil, fmt.Errorf("block not found: %s", blockID)
		}
		group.blocks = append(group.blocks, m)
	}
	return &group, nil
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

			firstBlockPartitionInfo, err := GetPartitionInfo(*group.blocks[0])
			if err != nil || firstBlockPartitionInfo == nil {
				firstBlockPartitionInfo = &PartitionInfo{
					PartitionedGroupID: 0,
					PartitionCount:     1,
					PartitionID:        0,
				}
			}
			for _, block := range group.blocks {
				blockPartitionInfo, err := GetPartitionInfo(*block)
				if err != nil || blockPartitionInfo == nil {
					blockPartitionInfo = &PartitionInfo{
						PartitionedGroupID: 0,
						PartitionCount:     1,
						PartitionID:        0,
					}
				}
				if blockPartitionInfo.PartitionedGroupID <= 0 || blockPartitionInfo.PartitionedGroupID != firstBlockPartitionInfo.PartitionedGroupID {
					groups = append(groups, group)
					continue nextGroup
				}
			}
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
		//if group.maxTime()-group.minTime() == group.rangeLength() {
		//	idx++
		//	continue
		//}

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

		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > group.rangeEnd {
			i++
			continue
		}

		if skipHighLevelBlock(m, tr) {
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

			if skipHighLevelBlock(blocks[i], tr) {
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

func skipHighLevelBlock(block *metadata.Meta, tr int64) bool {
	// Skip blocks that have rounded range equal to tr, and level > 1
	// Because tr is divisible by the previous tr, block range falls in
	// (tr/2, tr] should be rounded to tr.
	blockRange := block.MaxTime - block.MinTime
	return blockRange <= tr && blockRange > tr/2 && block.Compaction.Level > 1
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
