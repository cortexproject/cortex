package distributed_execution

// FragmentKey uniquely identifies a fragment of a distributed logical query plan.
// It combines a queryID (to identify the overall query) and a fragmentID
// (to identify the specific fragment within that query).
type FragmentKey struct {
	// queryID  identifies the distributed query this fragment belongs to
	queryID uint64
	// fragmentID identifies this specific fragment within the query
	fragmentID uint64
}

// MakeFragmentKey creates a new FragmentKey with the given queryID and fragmentID.
// It's used to track and identify fragments during distributed query execution.
func MakeFragmentKey(queryID uint64, fragmentID uint64) FragmentKey {
	return FragmentKey{
		queryID:    queryID,
		fragmentID: fragmentID,
	}
}

// GetQueryID returns the queryID for the current key
// This ID is shared across all fragments of the same distributed query.
func (f FragmentKey) GetQueryID() uint64 {
	return f.queryID
}

// GetFragmentID returns the ID for this specific fragment
// within its parent query.
func (f FragmentKey) GetFragmentID() uint64 {
	return f.fragmentID
}
