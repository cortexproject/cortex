package overrides

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/go-kit/log/level"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
)

const (
	// Error messages
	ErrInvalidLimits = "the following limits cannot be modified via the overrides API"
)

// No default allowed limits - these must be configured via runtime config

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

// validateHardLimits checks if the provided overrides exceed any hard limits from the runtime config
func (a *API) validateHardLimits(overrides map[string]any, userID string) error {
	// Read the runtime config to get hard limits
	reader, err := a.bucketClient.Get(context.Background(), a.runtimeConfigPath)
	if err != nil {
		level.Error(a.logger).Log("msg", "failed to read hard limits configuration", "userID", userID, "err", err)
		return fmt.Errorf("failed to validate hard limits")
	}
	defer reader.Close()

	var config runtimeconfig.RuntimeConfigValues
	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		level.Error(a.logger).Log("msg", "failed to decode hard limits configuration", "userID", userID, "err", err)
		return fmt.Errorf("failed to validate hard limits")
	}

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
		level.Error(a.logger).Log("msg", "failed to marshal hard limits", "userID", userID, "err", err)
		return fmt.Errorf("failed to validate hard limits")
	}

	var hardLimitsMap map[string]any
	if err := yaml.Unmarshal(yamlData, &hardLimitsMap); err != nil {
		level.Error(a.logger).Log("msg", "failed to unmarshal hard limits", "userID", userID, "err", err)
		return fmt.Errorf("failed to validate hard limits")
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
func (a *API) validateSingleHardLimit(limitName string, value, hardLimit any) error {
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
