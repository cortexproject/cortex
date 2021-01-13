package flux

// GroupMode represents the method for grouping data
type GroupMode int

const (
	// GroupModeNone indicates that no grouping action is specified
	GroupModeNone GroupMode = 0
	// GroupModeBy produces a table for each unique value of the specified GroupKeys.
	GroupModeBy GroupMode = 1 << iota
	// GroupModeExcept produces a table for the unique values of all keys, except those specified by GroupKeys.
	GroupModeExcept
)
