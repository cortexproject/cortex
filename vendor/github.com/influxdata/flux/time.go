package flux

import (
	"math"
	"time"

	"github.com/influxdata/flux/values"
)

var (
	MinTime = Time{
		Absolute: time.Unix(0, math.MinInt64),
	}
	MaxTime = Time{
		Absolute: time.Unix(0, math.MaxInt64),
	}
	Now = Time{
		IsRelative: true,
	}
)

// Time represents either a relative or absolute time.
// If Time is its zero value then it represents a time.Time{}.
// To represent the now time you must set IsRelative to true.
type Time struct {
	IsRelative bool
	Relative   time.Duration
	Absolute   time.Time
}

// Time returns the time specified relative to now.
func (t Time) Time(now time.Time) time.Time {
	if t.IsRelative {
		return now.Add(t.Relative)
	}
	return t.Absolute
}

func (t Time) IsZero() bool {
	return !t.IsRelative && t.Absolute.IsZero()
}

func (t *Time) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		t.Absolute = time.Time{}
		t.Relative = 0
		t.IsRelative = false
		return nil
	}

	str := string(data)
	if str == "now" {
		t.Relative = 0
		t.Absolute = time.Time{}
		t.IsRelative = true
		return nil
	}
	d, err := time.ParseDuration(str)
	if err == nil {
		t.Relative = d
		t.Absolute = time.Time{}
		t.IsRelative = true
		return nil
	}
	ts, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return err
	}
	t.Absolute = ts.UTC()
	t.IsRelative = false
	t.Relative = 0
	return nil
}

func (t Time) MarshalText() ([]byte, error) {
	if t.IsRelative {
		if t.Relative == 0 {
			return []byte("now"), nil
		}
		return []byte(t.Relative.String()), nil
	}
	return []byte(t.Absolute.Format(time.RFC3339Nano)), nil
}

// Duration is a marshalable duration type.
type Duration = values.Duration

// ConvertDurationNsecs will convert a time.Duration into a flux.Duration.
func ConvertDuration(v time.Duration) Duration {
	return values.ConvertDurationNsecs(v)
}
