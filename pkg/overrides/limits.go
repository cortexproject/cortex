package overrides

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
)

const (
	// Error messages
	ErrInvalidLimits = "the following limits cannot be modified via the overrides API"
)

// ValidateOverrides checks if the provided overrides only contain allowed limits
func ValidateOverrides(overrides map[string]any, allowedLimits []string) error {
	var invalidLimits []string

	for limitName := range overrides {
		if !slices.Contains(allowedLimits, limitName) {
			invalidLimits = append(invalidLimits, limitName)
		}
	}

	if len(invalidLimits) > 0 {
		return fmt.Errorf("%s: %s", ErrInvalidLimits, strings.Join(invalidLimits, ", "))
	}

	return nil
}

// validateHardLimits checks if the provided overrides exceed any hard limits from the runtime config.
func validateHardLimits(config runtimeconfig.RuntimeConfigValues, overrides map[string]any, userID string) error {
	// If no hard overrides are defined, allow the request
	if config.HardTenantLimits == nil {
		return nil
	}

	// Get hard limits for this specific user
	userHardLimits, exists := config.HardTenantLimits[userID]
	if !exists {
		return nil // No hard limits defined for this user
	}

	yamlData, err := yaml.Marshal(userHardLimits)
	if err != nil {
		return fmt.Errorf("failed to validate hard limits: %w", err)
	}

	var hardLimitsMap map[string]any
	if err := yaml.Unmarshal(yamlData, &hardLimitsMap); err != nil {
		return fmt.Errorf("failed to validate hard limits: %w", err)
	}

	// Validate each override against the user's hard limits
	for limitName, value := range overrides {
		if hardLimit, exists := hardLimitsMap[limitName]; exists {
			if err := validateSingleHardLimit(limitName, value, hardLimit); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateSingleHardLimit validates a single limit against its hard limit
func validateSingleHardLimit(limitName string, value, hardLimit any) error {
	// Convert both values to float64 for comparison
	valueFloat, err := convertToFloat64(value)
	if err != nil {
		return nil // Skip validation for unparseable values
	}

	hardLimitFloat, err := convertToFloat64(hardLimit)
	if err != nil {
		return nil // Skip validation for unparseable hard limits
	}

	// Hard limit is inclusive - values equal to the hard limit are allowed
	// For example, if hard limit is 100000, then 100000 is allowed but 100001 is not
	if valueFloat > hardLimitFloat {
		return fmt.Errorf("limit %s exceeds hard limit: %f > %f", limitName, valueFloat, hardLimitFloat)
	}

	return nil
}

// convertToFloat64 converts any value to float64
func convertToFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("unsupported type: %T", v)
	}
}
