package overrides

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	// Error messages
	ErrInvalidLimits = "the following limits cannot be modified via the overrides API"
)

// AllowedLimits defines the limits that can be modified via the overrides API
var AllowedLimits = []string{
	"max_global_series_per_user",
	"max_global_series_per_metric",
	"ingestion_rate",
	"ingestion_burst_size",
	"ruler_max_rules_per_rule_group",
	"ruler_max_rule_groups_per_tenant",
}

// ValidateOverrides checks if the provided overrides only contain allowed limits
func ValidateOverrides(overrides map[string]interface{}) error {
	var invalidLimits []string

	for limitName := range overrides {
		if !IsLimitAllowed(limitName) {
			invalidLimits = append(invalidLimits, limitName)
		}
	}

	if len(invalidLimits) > 0 {
		return fmt.Errorf("%s: %s", ErrInvalidLimits, strings.Join(invalidLimits, ", "))
	}

	return nil
}

// GetAllowedLimits returns a list of all allowed limit names
func GetAllowedLimits() []string {
	return AllowedLimits
}

// IsLimitAllowed checks if a specific limit can be modified
func IsLimitAllowed(limitName string) bool {
	for _, allowed := range AllowedLimits {
		if allowed == limitName {
			return true
		}
	}
	return false
}

// validateHardLimits checks if the provided overrides exceed any hard limits from the runtime config
func (a *API) validateHardLimits(overrides map[string]interface{}, userID string) error {
	// Read the runtime config to get hard limits
	reader, err := a.bucketClient.Get(context.Background(), a.runtimeConfigPath)
	if err != nil {
		// If we can't read the config, skip hard limit validation
		return nil
	}
	defer reader.Close()

	var config RuntimeConfigFile
	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		// If we can't decode the config, skip hard limit validation
		return nil
	}

	// If no hard overrides are defined, skip validation
	if config.HardOverrides == nil {
		return nil
	}

	// Get hard limits for this specific user
	userHardLimits, exists := config.HardOverrides[userID]
	if !exists {
		return nil // No hard limits defined for this user
	}

	// Validate each override against the user's hard limits
	for limitName, value := range overrides {
		if hardLimit, exists := userHardLimits[limitName]; exists {
			if err := a.validateSingleHardLimit(limitName, value, hardLimit); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateSingleHardLimit validates a single limit against its hard limit
func (a *API) validateSingleHardLimit(limitName string, value, hardLimit interface{}) error {
	// Convert both values to float64 for comparison
	valueFloat, err := convertToFloat64(value)
	if err != nil {
		return nil // Skip validation for unparseable values
	}

	hardLimitFloat, err := convertToFloat64(hardLimit)
	if err != nil {
		return nil // Skip validation for unparseable hard limits
	}

	if valueFloat > hardLimitFloat {
		return fmt.Errorf("limit %s exceeds hard limit: %f > %f", limitName, valueFloat, hardLimitFloat)
	}

	return nil
}

// convertToFloat64 converts any value to float64
func convertToFloat64(v interface{}) (float64, error) {
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
