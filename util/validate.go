package util

import (
	"fmt"
	"regexp"

	"github.com/prometheus/common/model"
)

var validLabelRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidateSample returns an err if the sample is invalid
func ValidateSample(s *model.Sample) error {
	for k, _ := range s.Metric {
		if !validLabelRE.MatchString(string(k)) {
			return fmt.Errorf("invalid label: %s", k)
		}
	}
	return nil
}
