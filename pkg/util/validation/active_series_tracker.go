package validation

import (
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	errActiveSeriesTrackerEmptyName     = errors.New("active series tracker name must not be empty")
	errActiveSeriesTrackerDuplicateName = errors.New("duplicate active series tracker name")
)

// ActiveSeriesTrackerConfig defines a single tracker entry that counts active series
// matching a set of label matchers. This is used for internal monitoring only —
// no limits are enforced.
type ActiveSeriesTrackerConfig struct {
	Name     string `yaml:"name" json:"name" doc:"nocli|description=Name of the tracker, used as a label value in the emitted metric."`
	Matchers string `yaml:"matchers" json:"matchers" doc:"nocli|description=PromQL series selector (e.g. {__name__=~\"api_.*\"}). All matchers must match for a series to be counted."`

	// Parsed matchers, populated during validation.
	parsedMatchers []*labels.Matcher `yaml:"-" json:"-" doc:"nocli"`
}

// ParsedMatchers returns the compiled matchers. Must call Validate() first.
func (c *ActiveSeriesTrackerConfig) ParsedMatchers() []*labels.Matcher {
	return c.parsedMatchers
}

// Validate parses the matchers string into compiled label matchers.
func (c *ActiveSeriesTrackerConfig) Validate() error {
	if c.Name == "" {
		return errActiveSeriesTrackerEmptyName
	}
	matchers, err := parser.ParseMetricSelector(c.Matchers)
	if err != nil {
		return fmt.Errorf("active series tracker %q: %w", c.Name, err)
	}
	c.parsedMatchers = matchers
	return nil
}

// ActiveSeriesTrackersConfig is a list of tracker configurations.
type ActiveSeriesTrackersConfig []ActiveSeriesTrackerConfig

// Validate parses and validates all tracker entries, ensuring names are unique and non-empty.
func (c ActiveSeriesTrackersConfig) Validate() error {
	names := make(map[string]struct{}, len(c))
	for i := range c {
		if err := c[i].Validate(); err != nil {
			return err
		}
		if _, exists := names[c[i].Name]; exists {
			return fmt.Errorf("%w: %q", errActiveSeriesTrackerDuplicateName, c[i].Name)
		}
		names[c[i].Name] = struct{}{}
	}
	return nil
}
