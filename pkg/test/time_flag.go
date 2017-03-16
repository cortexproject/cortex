package test

import (
	"time"
)

// TimeValue is a model.Time that can be used as a flag.
type TimeValue struct {
	time.Time
	set bool
}

// NewTimeValue makes a new DayValue; will round t down to the nearest midnight.
func NewTimeValue(t time.Time) TimeValue {
	return TimeValue{
		Time: t,
		set:  true,
	}
}

// String implements flag.Value
func (v TimeValue) String() string {
	return v.Time.Format(time.RFC3339)
}

// Set implements flag.Value
func (v *TimeValue) Set(s string) error {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}
	v.Time = t
	v.set = true
	return nil
}
