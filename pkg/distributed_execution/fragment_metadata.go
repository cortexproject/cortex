package distributed_execution

import (
	"context"
)

type fragmentMetadataKey struct{}

type fragmentMetadata struct {
	queryID       uint64
	fragmentID    uint64
	childIDToAddr map[uint64]string
	isRoot        bool
}

// InjectFragmentMetaData stores the distributed execution metadata for the current
// fragment into the context. This metadata is propagated from the query-scheduler to
// the querier so that the querier knows which fragment it is executing, where to pull
// its child fragments' results from, and whether it is the root (coordinator) fragment.
func InjectFragmentMetaData(ctx context.Context, fragmentID uint64, queryID uint64, isRoot bool, childIDToAddr map[uint64]string) context.Context {
	return context.WithValue(ctx, fragmentMetadataKey{}, fragmentMetadata{
		queryID:       queryID,
		fragmentID:    fragmentID,
		childIDToAddr: childIDToAddr,
		isRoot:        isRoot,
	})
}

// ExtractFragmentMetaData retrieves the distributed execution metadata for the current
// fragment from the context. The final return value reports whether metadata was present.
func ExtractFragmentMetaData(ctx context.Context) (isRoot bool, queryID uint64, fragmentID uint64, childAddrs map[uint64]string, ok bool) {
	metadata, ok := ctx.Value(fragmentMetadataKey{}).(fragmentMetadata)
	if !ok {
		return false, 0, 0, nil, false
	}
	return metadata.isRoot, metadata.queryID, metadata.fragmentID, metadata.childIDToAddr, true
}
