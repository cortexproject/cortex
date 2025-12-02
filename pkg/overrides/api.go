package overrides

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-kit/log/level"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// Error messages
	ErrInvalidJSON  = "invalid JSON"
	ErrUserNotFound = "user not found"

	// Runtime config errors
	ErrRuntimeConfig = "runtime config read error"
)

// getAllowedLimitsFromBucket reads allowed limits from the runtime config file
func (a *API) getAllowedLimitsFromBucket(ctx context.Context) ([]string, error) {
	reader, err := a.bucketClient.Get(ctx, a.runtimeConfigPath)
	if err != nil {
		return []string{}, nil // No allowed limits if config doesn't exist
	}
	defer reader.Close()

	var config runtimeconfig.RuntimeConfigValues
	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		level.Error(a.logger).Log("msg", "failed to decode runtime config", "err", err)
		return []string{}, fmt.Errorf("failed to decode runtime config")
	}

	return config.APIAllowedLimits, nil
}

// GetOverrides retrieves overrides for a specific tenant
func (a *API) GetOverrides(w http.ResponseWriter, r *http.Request) {
	userID, _, err := users.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Read overrides from bucket storage
	overrides, err := a.getOverridesFromBucket(r.Context(), userID)
	if err != nil {
		if err.Error() == ErrUserNotFound {
			http.Error(w, "user not found", http.StatusBadRequest)
		} else {
			level.Error(a.logger).Log("msg", "failed to get overrides from bucket", "userID", userID, "err", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
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
	userID, _, err := users.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var overrides map[string]any
	if err := json.NewDecoder(r.Body).Decode(&overrides); err != nil {
		http.Error(w, ErrInvalidJSON, http.StatusBadRequest)
		return
	}

	// Get allowed limits from runtime config
	allowedLimits, err := a.getAllowedLimitsFromBucket(r.Context())
	if err != nil {
		level.Error(a.logger).Log("msg", "failed to get allowed limits from bucket", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Validate that only allowed limits are being changed
	if err := ValidateOverrides(overrides, allowedLimits); err != nil {
		level.Error(a.logger).Log("msg", "invalid overrides validation", "userID", userID, "err", err)
		http.Error(w, "Invalid overrides", http.StatusBadRequest)
		return
	}

	// Validate that values don't exceed hard limits from runtime config
	if err := a.validateHardLimits(overrides, userID); err != nil {
		level.Error(a.logger).Log("msg", "hard limits validation failed", "userID", userID, "err", err)
		http.Error(w, "Invalid overrides", http.StatusBadRequest)
		return
	}

	// Write overrides to bucket storage
	if err := a.setOverridesToBucket(r.Context(), userID, overrides); err != nil {
		level.Error(a.logger).Log("msg", "failed to set overrides to bucket", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DeleteOverrides removes tenant-specific overrides
func (a *API) DeleteOverrides(w http.ResponseWriter, r *http.Request) {
	userID, _, err := users.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if err := a.deleteOverridesFromBucket(r.Context(), userID); err != nil {
		level.Error(a.logger).Log("msg", "failed to delete overrides from bucket", "userID", userID, "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// getOverridesFromBucket reads overrides for a specific tenant from the runtime config file
func (a *API) getOverridesFromBucket(ctx context.Context, userID string) (map[string]any, error) {
	reader, err := a.bucketClient.Get(ctx, a.runtimeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get runtime config: %w", err)
	}
	defer reader.Close()

	var config runtimeconfig.RuntimeConfigValues
	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		return nil, fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}

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
		// User does not exist in config - return error
		return nil, errors.New(ErrUserNotFound)
	}

	// No tenant limits configured - return empty map (no overrides)
	return map[string]any{}, nil
}

// setOverridesToBucket writes overrides for a specific tenant to the runtime config file
func (a *API) setOverridesToBucket(ctx context.Context, userID string, overrides map[string]any) error {
	var config runtimeconfig.RuntimeConfigValues
	reader, err := a.bucketClient.Get(ctx, a.runtimeConfigPath)
	if err == nil {
		defer reader.Close()
		if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
			return fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
		}
	}

	yamlData, err := yaml.Marshal(overrides)
	if err != nil {
		return fmt.Errorf("failed to marshal overrides: %w", err)
	}

	var limits validation.Limits
	if err := yaml.Unmarshal(yamlData, &limits); err != nil {
		return fmt.Errorf("invalid overrides format: %w", err)
	}

	if config.TenantLimits == nil {
		config.TenantLimits = make(map[string]*validation.Limits)
	}

	config.TenantLimits[userID] = &limits

	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}

	return a.bucketClient.Upload(ctx, a.runtimeConfigPath, bytes.NewReader(data))
}

// deleteOverridesFromBucket removes overrides for a specific tenant from the runtime config file
func (a *API) deleteOverridesFromBucket(ctx context.Context, userID string) error {
	reader, err := a.bucketClient.Get(ctx, a.runtimeConfigPath)
	if err != nil {
		return nil
	}
	defer reader.Close()

	var config runtimeconfig.RuntimeConfigValues
	if err := yaml.NewDecoder(reader).Decode(&config); err != nil {
		return fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}

	if config.TenantLimits != nil {
		delete(config.TenantLimits, userID)
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrRuntimeConfig, err)
	}

	return a.bucketClient.Upload(ctx, a.runtimeConfigPath, bytes.NewReader(data))
}
