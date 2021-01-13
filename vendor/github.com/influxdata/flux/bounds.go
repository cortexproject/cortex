package flux

import "time"

type Bounds struct {
	Start Time
	Stop  Time
	Now   time.Time
}

// IsEmpty reports whether the given bounds
// are empty, i.e., if start >= stop.
func (b Bounds) IsEmpty() bool {
	return b.Start.Time(b.Now).Equal(b.Stop.Time(b.Now)) || b.Start.Time(b.Now).After(b.Stop.Time(b.Now))
}

// HasZero returns true if the given bounds contain a Go zero time value as either Start or Stop.
func (b Bounds) HasZero() bool {
	return b.Start.IsZero() || b.Stop.IsZero()
}
