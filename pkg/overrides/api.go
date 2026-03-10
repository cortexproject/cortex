package overrides

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"

	"github.com/go-kit/log/level"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errUserNotFound = errors.New("user not found")
)

const (
	// Error messages
	ErrInvalidJSON = "invalid JSON"

	// Runtime config errors
	ErrRuntimeConfig = "runtime config read error"
)

// readRuntimeConfig reads and decodes the runtime config from bucket storage.
func (a *API) readRuntimeConfig(ctx context.Context) (runtimeconfig.RuntimeConfigValues, error) {
	var config runtimeconfig.RuntimeConfigValues
	reader, err := a.bucketClient.Get(ctx, a.cfg.LoadPath)
	if err != nil {
		return config, fmt.Errorf("failed to get runtime config: %w", err)
	}
	defer reader.Close()

	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		return config, fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}
	return config, nil
}

// GetOverrides retrieves overrides for a specific tenant
func (a *API) GetOverrides(w http.ResponseWriter, r *http.Request) {
	userID, _, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	config, err := a.readRuntimeConfig(r.Context())
	if err != nil {
		level.Error(a.logger).Log("msg", "failed to read runtime config", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	overrides, err := extractTenantOverrides(config, userID)
	if err != nil {
		if errors.Is(err, errUserNotFound) {
			level.Info(a.logger).Log("msg", "User not found", "user", userID)
			http.Error(w, "user not found", http.StatusBadRequest)
		} else {
			level.Error(a.logger).Log("msg", "failed to get overrides", "userID", userID, "err", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}
	if len(overrides) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(overrides); err != nil {
		level.Error(a.logger).Log("msg", "failed to encode overrides response", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// SetOverrides updates overrides for a specific tenant
func (a *API) SetOverrides(w http.ResponseWriter, r *http.Request) {
	userID, _, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var overrides map[string]any
	if err := json.NewDecoder(r.Body).Decode(&overrides); err != nil {
		http.Error(w, ErrInvalidJSON, http.StatusBadRequest)
		return
	}

	// Read runtime config once for all validations and the write
	config, err := a.readRuntimeConfig(r.Context())
	if err != nil {
		level.Error(a.logger).Log("msg", "failed to read runtime config", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Validate that only allowed limits are being changed
	if err := ValidateOverrides(overrides, config.APIAllowedLimits); err != nil {
		level.Error(a.logger).Log("msg", "invalid overrides validation", "userID", userID, "err", err)
		http.Error(w, "Invalid overrides", http.StatusBadRequest)
		return
	}

	// Validate that values don't exceed hard limits from runtime config
	if err := validateHardLimits(config, overrides, userID); err != nil {
		level.Error(a.logger).Log("msg", "hard limits validation failed", "userID", userID, "err", err)
		http.Error(w, "Invalid overrides", http.StatusBadRequest)
		return
	}

	// Write overrides to bucket storage
	if err := a.writeOverridesToBucket(r.Context(), config, userID, overrides); err != nil {
		level.Error(a.logger).Log("msg", "failed to set overrides to bucket", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DeleteOverrides removes tenant-specific overrides
func (a *API) DeleteOverrides(w http.ResponseWriter, r *http.Request) {
	userID, _, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := a.deleteOverridesFromBucket(r.Context(), userID); err != nil {
		level.Error(a.logger).Log("msg", "failed to delete overrides from bucket", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// extractTenantOverrides extracts overrides for a specific tenant from a decoded runtime config.
func extractTenantOverrides(config runtimeconfig.RuntimeConfigValues, userID string) (map[string]any, error) {
	if config.TenantLimits != nil {
		if tenantLimits, exists := config.TenantLimits[userID]; exists {
			// Use YAML marshaling to convert validation.Limits to map[string]interface{}
			// This follows the same pattern as the existing runtime config handler
			yamlData, err := yaml.Marshal(tenantLimits)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal limits: %w", err)
			}

			var result map[string]any
			if err := yaml.Unmarshal(yamlData, &result); err != nil {
				return nil, fmt.Errorf("failed to unmarshal limits: %w", err)
			}

			return result, nil
		}
		return nil, errUserNotFound
	}

	// No tenant limits configured - return empty map (no overrides)
	return map[string]any{}, nil
}

// mergeLimits merges new overrides into existing limits
func mergeLimits(existing map[string]any, overrides map[string]any) map[string]any {
	if existing == nil {
		return overrides
	}

	merged := make(map[string]any)

	// Copy existing limits
	maps.Copy(merged, existing)

	// Override with new values
	maps.Copy(merged, overrides)

	return merged
}

// writeOverridesToBucket writes overrides for a specific tenant to the runtime config file
// using a pre-read config to avoid redundant bucket reads.
func (a *API) writeOverridesToBucket(ctx context.Context, config runtimeconfig.RuntimeConfigValues, userID string, overrides map[string]any) error {
	if config.TenantLimits == nil {
		config.TenantLimits = make(map[string]*validation.Limits)
	}

	// Get existing limits for the user
	var existingLimitsMap map[string]any
	if existingLimits, exists := config.TenantLimits[userID]; exists && existingLimits != nil {
		// Convert existing limits to map
		yamlData, err := yaml.Marshal(existingLimits)
		if err != nil {
			return fmt.Errorf("failed to marshal existing limits: %w", err)
		}

		if err := yaml.Unmarshal(yamlData, &existingLimitsMap); err != nil {
			return fmt.Errorf("failed to unmarshal existing limits: %w", err)
		}
	}

	// Merge existing limits with new overrides
	mergedLimits := mergeLimits(existingLimitsMap, overrides)

	// Convert merged limits back to validation.Limits
	yamlData, err := yaml.Marshal(mergedLimits)
	if err != nil {
		return fmt.Errorf("failed to marshal overrides: %w", err)
	}

	var limits validation.Limits
	if err := yaml.Unmarshal(yamlData, &limits); err != nil {
		return fmt.Errorf("invalid overrides format: %w", err)
	}

	config.TenantLimits[userID] = &limits

	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}

	return a.bucketClient.Upload(ctx, a.cfg.LoadPath, bytes.NewReader(data))
}

// deleteOverridesFromBucket removes overrides for a specific tenant from the runtime config file
func (a *API) deleteOverridesFromBucket(ctx context.Context, userID string) error {
	config, err := a.readRuntimeConfig(ctx)
	if err != nil {
		return err
	}

	if config.TenantLimits != nil {
		delete(config.TenantLimits, userID)
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}

	return a.bucketClient.Upload(ctx, a.cfg.LoadPath, bytes.NewReader(data))
}
