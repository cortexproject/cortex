package util

import (
	"flag"
	"net/url"
	"time"

	"github.com/prometheus/common/model"
)

// Registerer is a thing that can RegisterFlags
type Registerer interface {
	RegisterFlags(*flag.FlagSet)
}

// RegisterFlags registers flags with the provided Registerers
func RegisterFlags(rs ...Registerer) {
	for _, r := range rs {
		r.RegisterFlags(flag.CommandLine)
	}
}

// TimeValue is a time.Time that can be used as a flag.
type TimeValue struct {
	time.Time
}

// String implements flag.Value
func (v TimeValue) String() string {
	return v.Time.Format(time.RFC3339)
}

// Set implements flag.Value
func (v *TimeValue) Set(s string) error {
	var err error
	v.Time, err = time.Parse(time.RFC3339, s)
	return err
}

// DayValue is a model.Time that can be used as a flag.
// NB it only parses days!
type DayValue struct {
	model.Time
}

// String implements flag.Value
func (v DayValue) String() string {
	return v.Time.Time().Format(time.RFC3339)
}

// Set implements flag.Value
func (v *DayValue) Set(s string) error {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return err
	}
	v.Time = model.TimeFromUnix(t.Unix())
	return nil
}

// URLValue is a url.URL that can be used as a flag.
type URLValue struct {
	*url.URL
}

// String implements flag.Value
func (v URLValue) String() string {
	if v.URL == nil {
		return ""
	}
	return v.URL.String()
}

// Set implements flag.Value
func (v *URLValue) Set(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	v.URL = u
	return nil
}
