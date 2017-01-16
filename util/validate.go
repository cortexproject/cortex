package util

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/prometheus/common/model"
)

var validLabelRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidateSample returns an err if the sample is invalid
func ValidateSample(s *model.Sample) error {
	for k, v := range s.Metric {
		if !validLabelRE.MatchString(string(k)) {
			return fmt.Errorf("invalid label: %s", k)
		}

		if strings.ContainsRune(string(v), '\x00') {
			return fmt.Errorf("invalid label value: %s", v)
		}
	}

	return nil
}
