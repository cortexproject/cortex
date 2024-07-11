package testutils

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"

	"github.com/cortexproject/cortex/pkg/compactor"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func GenerateBlock(ctx context.Context, logger log.Logger, cfg BlockGenConfig, storageCfg tsdb.BlocksStorageConfig) error {
	seriesGroups := generateSeriesGroups(cfg)
	partitionedGroupID := generatePartitionedGroupID(cfg.WorkspaceID, cfg.BlockStartTime.Time.UnixMilli(), cfg.BlockEndTime.Time.UnixMilli())
	for seriesGroupIdx, seriesGroup := range seriesGroups {
		blockID, err := e2eutil.CreateBlock(
			ctx,
			cfg.OutputDirectory,
			seriesGroup,
			cfg.SamplesPerSeries,
			cfg.BlockStartTime.Time.UnixMilli(),
			cfg.BlockEndTime.Time.UnixMilli(),
			labels.Labels{
				{Name: "__org_id__", Value: cfg.WorkspaceID},
			},
			0,
			metadata.NoneFunc)
		if err != nil {
			level.Error(logger).Log("msg", "unable to create block", "series_group_idx", seriesGroupIdx, "err", err)
			return errors.Wrapf(err, "unable to create block")
		}
		if err = updateMeta(logger, blockID, seriesGroupIdx, partitionedGroupID, cfg); err != nil {
			level.Error(logger).Log("msg", "unable to update meta for block", "series_group_idx", seriesGroupIdx, "err", err)
			return errors.Wrapf(err, "unable to update meta for block")
		}
		level.Info(logger).Log("msg", "block created", "series_group_idx", seriesGroupIdx, "block_id", blockID.String())
		err = uploadBlock(ctx, logger, cfg, storageCfg, blockID)
		if err != nil {
			level.Error(logger).Log("msg", "failed to upload block", "series_group_idx", seriesGroupIdx, "block_id", blockID.String())
			return errors.Wrapf(err, "failed to upload block")
		}
		level.Info(logger).Log("msg", "uploaded block", "series_group_idx", seriesGroupIdx, "block_id", blockID.String())
	}
	return nil
}

func generateSeriesGroups(cfg BlockGenConfig) map[int][]labels.Labels {
	seriesGroups := make(map[int][]labels.Labels)
	for i := 0; i < cfg.MetricsPerWorkspace; i++ {
		for j := 0; j < cfg.ActiveSeriesPerMetric; j++ {
			metricLabels := labels.Labels{
				{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)},
				{Name: fmt.Sprintf("extra_label_%d", j), Value: fmt.Sprintf("extra_label_value_%d", j)},
			}
			if cfg.CompactionLevel > 1 {
				partitionID := int(metricLabels.Hash() % uint64(cfg.PartitionCount))
				seriesGroups[partitionID] = append(seriesGroups[partitionID], metricLabels)
			} else {
				shardID := (cfg.ActiveSeriesPerMetric*i + j) % cfg.L1ShardSize
				seriesGroups[shardID] = append(seriesGroups[shardID], metricLabels)
			}
		}
	}
	return seriesGroups
}

func updateMeta(logger log.Logger, blockID ulid.ULID, seriesGroupIdx int, partitionedGroupID uint32, cfg BlockGenConfig) error {
	blockDir := filepath.Join(cfg.OutputDirectory, blockID.String())
	newMeta, err := metadata.ReadFromDir(blockDir)
	if err != nil {
		return err
	}
	newMeta.Compaction.Level = cfg.CompactionLevel
	if cfg.CompactionLevel > 1 {
		newMeta.Thanos.Extensions = compactor.CortexMetaExtensions{
			PartitionInfo: &compactor.PartitionInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionID:        seriesGroupIdx,
				PartitionCount:     cfg.PartitionCount,
			},
		}
	} else {
		newMeta.Thanos.Labels["__ingester_id__"] = fmt.Sprintf("ingester-%d", seriesGroupIdx)
	}
	if err := newMeta.WriteToDir(logger, blockDir); err != nil {
		return err
	}
	return nil
}

func generatePartitionedGroupID(workspaceID string, rangeStart int64, rangeEnd int64) uint32 {
	groupString := fmt.Sprintf("%v%v%v", workspaceID, rangeStart, rangeEnd)
	groupHasher := fnv.New32a()
	_, _ = groupHasher.Write([]byte(groupString))
	groupHash := groupHasher.Sum32()
	return groupHash
}

func uploadBlock(ctx context.Context, logger log.Logger, cfg BlockGenConfig, storageCfg tsdb.BlocksStorageConfig, blockID ulid.ULID) error {
	blockDir := filepath.Join(cfg.OutputDirectory, blockID.String())
	bkt, err := bucket.NewClient(ctx, storageCfg.Bucket, "generate-block", logger, prometheus.NewPedanticRegistry())
	if err != nil {
		return err
	}
	bkt = bucket.NewPrefixedBucketClient(bkt, cfg.WorkspaceID)
	return block.Upload(ctx, logger, bkt, blockDir, metadata.NoneFunc, objstore.WithUploadConcurrency(4))
}

type BlockGenConfig struct {
	OutputDirectory       string                    `yaml:"output_directory"`
	WorkspaceID           string                    `yaml:"workspace_id"`
	MetricsPerWorkspace   int                       `yaml:"metrics_per_workspace"`
	ActiveSeriesPerMetric int                       `yaml:"active_series_per_metric"`
	SamplesPerSeries      int                       `yaml:"samples_per_series"`
	BlockStartTime        model.TimeOrDurationValue `yaml:"block_start_time"`
	BlockEndTime          model.TimeOrDurationValue `yaml:"block_end_time"`
	CompactionLevel       int                       `yaml:"compaction_level"`
	PartitionCount        int                       `yaml:"partition_count"`
	L1ShardSize           int                       `yaml:"shard_size"`
}

func (c *BlockGenConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.OutputDirectory, "output-directory", "", "Path to output directory of generated blocks on local disk")
	f.StringVar(&c.WorkspaceID, "workspace-id", "", "Workspace ID")
	f.IntVar(&c.MetricsPerWorkspace, "metrics-per-workspace", 0, "Number of metrics per workspace")
	f.IntVar(&c.ActiveSeriesPerMetric, "active-series-per-metric", 0, "Number of series per metric")
	f.IntVar(&c.SamplesPerSeries, "samples-per-series", 0, "Number of samples per series")
	f.Var(&c.BlockStartTime, "block-start-time", "Start time of generated block. It can be in either RFC3339 timestamp format or relative time compared to now")
	f.Var(&c.BlockEndTime, "block-end-time", "End time of generated block. It can be in either RFC3339 timestamp format or relative time compared to now")
	f.IntVar(&c.CompactionLevel, "compaction-level", 1, "Compaction level of generated block")
	f.IntVar(&c.PartitionCount, "partition-count", 1, "Number of partitions generated block would be split into. This only works when CompactionLevel is greater than 1")
	f.IntVar(&c.L1ShardSize, "shard-size", 1, "Number of shards. This only works when CompactionLevel set to 1")
}
