package e2ecortex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cortexproject/cortex/pkg/configs/userconfig"
)

// ConfigsClient drives the configs API (target=configs). It is intentionally separate
// from Client because the configs service is a distinct deployment with its own URL
// and an alertmanager_config endpoint that would otherwise collide with the
// multitenant alertmanager helpers on Client.
type ConfigsClient struct {
	endpoint   string // host:port of the configs service HTTP API
	orgID      string
	timeout    time.Duration
	httpClient *http.Client
}

// NewConfigsClient builds a client for the configs API hosted at endpoint
// (e.g. "127.0.0.1:1234"). orgID may be empty for endpoints that don't require
// tenant authentication (e.g. the private/admin routes).
func NewConfigsClient(endpoint, orgID string) *ConfigsClient {
	return &ConfigsClient{
		endpoint:   endpoint,
		orgID:      orgID,
		timeout:    30 * time.Second,
		httpClient: &http.Client{},
	}
}

// ConfigsView mirrors api.ConfigsView for unmarshaling the /private endpoint response.
// Duplicated here so the integration package doesn't pull in pkg/configs/api just for a struct.
type ConfigsView struct {
	Configs map[string]userconfig.View `json:"configs"`
}

func (c *ConfigsClient) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://%s%s", c.endpoint, path), body)
	if err != nil {
		return nil, nil, err
	}
	if c.orgID != "" {
		req.Header.Set("X-Scope-OrgID", c.orgID)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, err
	}
	return resp, respBody, nil
}

// GetRulesConfig fetches the rules config for the client's tenant.
func (c *ConfigsClient) GetRulesConfig(ctx context.Context) (*userconfig.View, int, error) {
	return c.getConfig(ctx, "/api/prom/configs/rules")
}

// PostRulesConfig stores a new version of the rules config for the client's tenant.
// It returns the HTTP status code and response body so callers can assert on the
// status and surface the body for debugging on unexpected responses.
func (c *ConfigsClient) PostRulesConfig(ctx context.Context, cfg userconfig.Config) (int, []byte, error) {
	return c.postConfig(ctx, "/api/prom/configs/rules", cfg)
}

// GetAlertmanagerConfig fetches the alertmanager config for the client's tenant.
// Named to avoid colliding with Client.GetAlertmanagerConfig, which targets the
// multitenant alertmanager service rather than the configs service.
func (c *ConfigsClient) GetAlertmanagerConfig(ctx context.Context) (*userconfig.View, int, error) {
	return c.getConfig(ctx, "/api/prom/configs/alertmanager")
}

// PostAlertmanagerConfig stores a new version of the alertmanager config for the client's tenant.
// It returns the HTTP status code and response body (see PostRulesConfig).
func (c *ConfigsClient) PostAlertmanagerConfig(ctx context.Context, cfg userconfig.Config) (int, []byte, error) {
	return c.postConfig(ctx, "/api/prom/configs/alertmanager", cfg)
}

// GetAllRulesConfigs hits the admin endpoint that returns every tenant's newest
// rules config keyed by tenant ID.
func (c *ConfigsClient) GetAllRulesConfigs(ctx context.Context) (map[string]userconfig.View, int, error) {
	return c.getAllRulesConfigs(ctx, "/private/api/prom/configs/rules")
}

// GetAllRulesConfigsSince hits the admin endpoint with a ?since=<id> filter,
// returning only configs whose version ID is greater than since.
func (c *ConfigsClient) GetAllRulesConfigsSince(ctx context.Context, since userconfig.ID) (map[string]userconfig.View, int, error) {
	return c.getAllRulesConfigs(ctx, fmt.Sprintf("/private/api/prom/configs/rules?since=%d", since))
}

func (c *ConfigsClient) getAllRulesConfigs(ctx context.Context, path string) (map[string]userconfig.View, int, error) {
	resp, body, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, nil
	}
	var view ConfigsView
	if err := json.Unmarshal(body, &view); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("decoding response: %w", err)
	}
	return view.Configs, resp.StatusCode, nil
}

func (c *ConfigsClient) getConfig(ctx context.Context, path string) (*userconfig.View, int, error) {
	resp, body, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, nil
	}
	var view userconfig.View
	if err := json.Unmarshal(body, &view); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("decoding response: %w", err)
	}
	return &view, resp.StatusCode, nil
}

func (c *ConfigsClient) postConfig(ctx context.Context, path string, cfg userconfig.Config) (int, []byte, error) {
	buf, err := json.Marshal(cfg)
	if err != nil {
		return 0, nil, err
	}
	resp, body, err := c.do(ctx, http.MethodPost, path, bytes.NewReader(buf))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}
