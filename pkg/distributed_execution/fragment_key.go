package distributed_execution

type FragmentKey struct {
	queryID    uint64
	fragmentID uint64
}

func MakeFragmentKey(queryID uint64, fragmentID uint64) *FragmentKey {
	return &FragmentKey{
		queryID:    queryID,
		fragmentID: fragmentID,
	}
}

func (f FragmentKey) GetQueryID() uint64 {
	return f.queryID
}

func (f FragmentKey) GetFragmentID() uint64 {
	return f.fragmentID
}
