package tsdb

import (
	"fmt"
	"strconv"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type CortexMetaExtensions struct {
	PartitionInfo *PartitionInfo `json:"partition_info,omitempty"`
	TimeRange     int64          `json:"time_range,omitempty"`
}

type PartitionInfo struct {
	PartitionedGroupID           uint32 `json:"partitioned_group_id"`
	PartitionCount               int    `json:"partition_count"`
	PartitionID                  int    `json:"partition_id"`
	PartitionedGroupCreationTime int64  `json:"partitioned_group_creation_time"`
}

var (
	DefaultPartitionInfo = PartitionInfo{
		PartitionedGroupID:           0,
		PartitionID:                  0,
		PartitionCount:               1,
		PartitionedGroupCreationTime: 0,
	}
)

func (c *CortexMetaExtensions) TimeRangeStr() string {
	return strconv.FormatInt(c.TimeRange, 10)
}

func ConvertToCortexMetaExtensions(extensions any) (*CortexMetaExtensions, error) {
	defaultPartitionInfo := DefaultPartitionInfo
	cortexExtensions, err := metadata.ConvertExtensions(extensions, &CortexMetaExtensions{
		PartitionInfo: &defaultPartitionInfo,
	})

	if err != nil {
		return nil, err
	}
	if cortexExtensions == nil {
		return nil, nil
	}
	converted, ok := cortexExtensions.(*CortexMetaExtensions)
	if !ok {
		return nil, fmt.Errorf("unable to convert extensions to CortexMetaExtensions")
	}
	return converted, nil
}

func ConvertToPartitionInfo(extensions any) (*PartitionInfo, error) {
	cortexExtensions, err := ConvertToCortexMetaExtensions(extensions)
	if err != nil {
		return nil, err
	}
	if cortexExtensions == nil {
		return nil, nil
	}
	return cortexExtensions.PartitionInfo, nil
}

func GetCortexMetaExtensionsFromMeta(meta metadata.Meta) (*CortexMetaExtensions, error) {
	return ConvertToCortexMetaExtensions(meta.Thanos.Extensions)
}

func GetPartitionInfo(meta metadata.Meta) (*PartitionInfo, error) {
	return ConvertToPartitionInfo(meta.Thanos.Extensions)
}
