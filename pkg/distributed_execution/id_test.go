package distributed_execution

import (
	"context"
	"reflect"
	"testing"
)

func TestFragmentMetadata(t *testing.T) {
	tests := []struct {
		name      string
		queryID   uint64
		fragID    uint64
		isRoot    bool
		childIDs  []uint64
		childAddr []string
	}{
		{
			name:      "basic test",
			queryID:   123,
			fragID:    456,
			isRoot:    true,
			childIDs:  []uint64{1, 2, 3},
			childAddr: []string{"addr1", "addr2", "addr3"},
		},
		{
			name:      "empty children",
			queryID:   789,
			fragID:    101,
			isRoot:    false,
			childIDs:  []uint64{},
			childAddr: []string{},
		},
		{
			name:      "single child",
			queryID:   999,
			fragID:    888,
			isRoot:    true,
			childIDs:  []uint64{42},
			childAddr: []string{"[IP_ADDRESS]:8080"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// injection
			ctx := context.Background()

			childIDToAddr := make(map[uint64]string)
			for i, childID := range tt.childIDs {
				childIDToAddr[childID] = tt.childAddr[i]
			}
			newCtx := InjectFragmentMetaData(ctx, tt.fragID, tt.queryID, tt.isRoot, childIDToAddr)

			// extraction
			isRoot, queryID, fragmentID, childAddrs, ok := ExtractFragmentMetaData(newCtx)

			// verify results
			if !ok {
				t.Error("ExtractFragmentMetaData failed, ok = false")
			}

			if isRoot != tt.isRoot {
				t.Errorf("isRoot = %v, want %v", isRoot, tt.isRoot)
			}

			if queryID != tt.queryID {
				t.Errorf("queryID = %v, want %v", queryID, tt.queryID)
			}

			if fragmentID != tt.fragID {
				t.Errorf("fragmentID = %v, want %v", fragmentID, tt.fragID)
			}

			// create expected childIDToAddr map
			expectedChildAddrs := make(map[uint64]string)
			for i, childID := range tt.childIDs {
				expectedChildAddrs[childID] = tt.childAddr[i]
			}

			if !reflect.DeepEqual(childAddrs, expectedChildAddrs) {
				t.Errorf("childAddrs = %v, want %v", childAddrs, expectedChildAddrs)
			}
		})
	}
}

func TestExtractFragmentMetaDataWithEmptyContext(t *testing.T) {
	ctx := context.Background()
	isRoot, queryID, fragmentID, childAddrs, ok := ExtractFragmentMetaData(ctx)
	if ok {
		t.Error("ExtractFragmentMetaData should return ok=false for empty context")
	}
	if isRoot || queryID != 0 || fragmentID != 0 || childAddrs != nil {
		t.Error("ExtractFragmentMetaData should return zero values for empty context")
	}
}
