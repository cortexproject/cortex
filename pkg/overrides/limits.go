package overrides

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
)

const (
	// Error messages
	ErrInvalidLimits = "the following limits cannot be modified via the overrides API"
)

// No default allowed limits - these must be configured via runtime config

// ValidateOverrides checks if the provided overrides only contain allowed limits
func ValidateOverrides(overrides map[string]interface{}, allowedLimits []string) error {
	var invalidLimits []string

	for limitName := range overrides {
		if !IsLimitAllowed(limitName, allowedLimits) {
			invalidLimits = append(invalidLimits, limitName)
		}
	}

	if len(invalidLimits) > 0 {
		return fmt.Errorf("%s: %s", ErrInvalidLimits, strings.Join(invalidLimits, ", "))
	}

	return nil
}

// GetAllowedLimits returns the allowed limits from runtime config
// If no allowed limits are configured, returns empty slice (no limits allowed)
func GetAllowedLimits(allowedLimits []string) []string {
	return allowedLimits
}

// IsLimitAllowed checks if a specific limit can be modified
func IsLimitAllowed(limitName string, allowedLimits []string) bool {
	for _, allowed := range allowedLimits {
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

	var config runtimeconfig.RuntimeConfigValues
	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		// If we can't decode the config, skip hard limit validation
		return nil
	}

	// If no hard overrides are defined, skip validation
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
		return nil // Skip validation if we can't marshal
	}

	var hardLimitsMap map[string]interface{}
	if err := yaml.Unmarshal(yamlData, &hardLimitsMap); err != nil {
		return nil // Skip validation if we can't unmarshal
	}

	// Validate each override against the user's hard limits
	for limitName, value := range overrides {
		if hardLimit, exists := hardLimitsMap[limitName]; exists {
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
