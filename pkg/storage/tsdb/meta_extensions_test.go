package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestGetPartitionedInfo(t *testing.T) {
	for _, tcase := range []struct {
		name     string
		meta     metadata.Meta
		expected *PartitionInfo
	}{
		{
			name: "partition info with all information provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: &CortexMetaExtensions{
						PartitionInfo: &PartitionInfo{
							PartitionedGroupID: 123,
							PartitionID:        8,
							PartitionCount:     32,
						},
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 123,
				PartitionID:        8,
				PartitionCount:     32,
			},
		},
		{
			name: "partition info with only PartitionedGroupID provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: &CortexMetaExtensions{
						PartitionInfo: &PartitionInfo{
							PartitionedGroupID: 123,
						},
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 123,
				PartitionID:        0,
				PartitionCount:     0,
			},
		},
		{
			name: "partition info with only PartitionID provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: &CortexMetaExtensions{
						PartitionInfo: &PartitionInfo{
							PartitionID: 5,
						},
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 0,
				PartitionID:        5,
				PartitionCount:     0,
			},
		},
		{
			name: "partition info with only PartitionCount provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: &CortexMetaExtensions{
						PartitionInfo: &PartitionInfo{
							PartitionCount: 4,
						},
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 0,
				PartitionID:        0,
				PartitionCount:     4,
			},
		},
		{
			name: "meta with empty partition info provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: &CortexMetaExtensions{
						PartitionInfo: &PartitionInfo{},
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 0,
				PartitionID:        0,
				PartitionCount:     0,
			},
		},
		{
			name: "meta with nil partition info provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: &CortexMetaExtensions{
						PartitionInfo: nil,
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 0,
				PartitionID:        0,
				PartitionCount:     1,
			},
		},
		{
			name: "meta with non CortexMetaExtensions provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: struct {
						dummy string
					}{
						dummy: "test_dummy",
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 0,
				PartitionID:        0,
				PartitionCount:     1,
			},
		},
		{
			name: "meta with invalid CortexMetaExtensions provided",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: struct {
						PartitionInfo struct {
							PartitionedGroupID uint32 `json:"partitionedGroupId"`
							PartitionCount     int    `json:"partitionCount"`
							PartitionID        int    `json:"partitionId"`
						} `json:"partition_info,omitempty"`
					}{
						PartitionInfo: struct {
							PartitionedGroupID uint32 `json:"partitionedGroupId"`
							PartitionCount     int    `json:"partitionCount"`
							PartitionID        int    `json:"partitionId"`
						}{
							PartitionedGroupID: 123,
							PartitionID:        8,
							PartitionCount:     32,
						},
					},
				},
			},
			expected: &PartitionInfo{
				PartitionedGroupID: 0,
				PartitionID:        0,
				PartitionCount:     1,
			},
		},
		{
			name: "meta does not have any extensions",
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Extensions: nil,
				},
			},
			expected: nil,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			result, err := GetPartitionInfo(tcase.meta)
			assert.NoError(t, err)
			if tcase.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, *tcase.expected, *result)
			}
		})
	}
}
