package compactor

import (
	"context"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestLabelRemoverFilter(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)

	tests := map[string]struct {
		labels   []string
		input    map[ulid.ULID]map[string]string
		expected map[ulid.ULID]map[string]string
	}{
		"should remove cpnfigured labels": {
			labels: []string{cortex_tsdb.IngesterIDExternalLabel},
			input: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
			expected: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			metas := map[ulid.ULID]*metadata.Meta{}
			for id, lbls := range testData.input {
				metas[id] = &metadata.Meta{Thanos: metadata.Thanos{Labels: lbls}}
			}

			f := NewLabelRemoverFilter(testData.labels)
			err := f.Filter(context.Background(), metas, nil, nil)
			require.NoError(t, err)
			assert.Len(t, metas, len(testData.expected))

			for expectedID, expectedLbls := range testData.expected {
				assert.NotNil(t, metas[expectedID])
				assert.Equal(t, expectedLbls, metas[expectedID].Thanos.Labels)
			}
		})
	}
}
