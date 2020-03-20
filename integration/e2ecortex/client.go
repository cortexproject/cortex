package e2ecortex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	alertConfig "github.com/prometheus/alertmanager/config"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	yaml "gopkg.in/yaml.v2"

	rulefmt "github.com/cortexproject/cortex/pkg/ruler/legacy_rulefmt"
)

// Client is a client used to interact with Cortex in integration tests
type Client struct {
	alertmanagerClient promapi.Client
	rulerAddress       string
	distributorAddress string
	timeout            time.Duration
	httpClient         *http.Client
	querierClient      promv1.API
	orgID              string
}

// NewClient makes a new Cortex client
func NewClient(
	distributorAddress string,
	querierAddress string,
	alertmanagerAddress string,
	rulerAddress string,
	orgID string,
) (*Client, error) {
	// Create querier API client
	querierAPIClient, err := promapi.NewClient(promapi.Config{
		Address:      "http://" + querierAddress + "/api/prom",
		RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: http.DefaultTransport},
	})
	if err != nil {
		return nil, err
	}

	c := &Client{
		distributorAddress: distributorAddress,
		rulerAddress:       rulerAddress,
		timeout:            5 * time.Second,
		httpClient:         &http.Client{},
		querierClient:      promv1.NewAPI(querierAPIClient),
		orgID:              orgID,
	}

	if alertmanagerAddress != "" {
		alertmanagerAPIClient, err := promapi.NewClient(promapi.Config{
			Address:      "http://" + alertmanagerAddress + "/api/prom",
			RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: http.DefaultTransport},
		})
		if err != nil {
			return nil, err
		}
		c.alertmanagerClient = alertmanagerAPIClient
	}

	return c, nil
}

// Push the input timeseries to the remote endpoint
func (c *Client) Push(timeseries []prompb.TimeSeries) (*http.Response, error) {
	// Create write request
	data, err := proto.Marshal(&prompb.WriteRequest{Timeseries: timeseries})
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	compressed := snappy.Encode(nil, data)
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/prom/push", c.distributorAddress), bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return res, nil
}

// Query runs a query
func (c *Client) Query(query string, ts time.Time) (model.Value, error) {
	value, _, err := c.querierClient.Query(context.Background(), query, ts)
	return value, err
}

type addOrgIDRoundTripper struct {
	orgID string
	next  http.RoundTripper
}

func (r *addOrgIDRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", r.orgID)

	return r.next.RoundTrip(req)
}

// ServerStatus represents a Alertmanager status response
// TODO: Upgrade to Alertmanager v0.20.0+ and utilize vendored structs
type ServerStatus struct {
	Data struct {
		ConfigYaml string `json:"configYAML"`
	} `json:"data"`
}

// GetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) GetAlertmanagerConfig(ctx context.Context) (*alertConfig.Config, error) {
	u := c.alertmanagerClient.URL("/api/v1/status", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	_, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	var ss *ServerStatus
	err = json.Unmarshal(body, &ss)
	if err != nil {
		return nil, err
	}

	cfg := &alertConfig.Config{}
	err = yaml.Unmarshal([]byte(ss.Data.ConfigYaml), cfg)

	return cfg, err
}

// GetRuleGroups gets the status of an alertmanager instance
func (c *Client) GetRuleGroups() (map[string][]rulefmt.RuleGroup, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/prom/rules", c.rulerAddress), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	rgs := map[string][]rulefmt.RuleGroup{}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, rgs)
	if err != nil {
		return nil, err
	}

	return rgs, nil
}

// SetRuleGroup gets the status of an alertmanager instance
func (c *Client) SetRuleGroup(rulegroup rulefmt.RuleGroup, namespace string) error {
	// Create write request
	data, err := yaml.Marshal(rulegroup)
	if err != nil {
		return err
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/prom/rules/%s", c.rulerAddress, url.PathEscape(namespace)), bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer res.Body.Close()
	return nil
}

// DeleteRuleGroup gets the status of an alertmanager instance
func (c *Client) DeleteRuleGroup(namespace string, groupName string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/prom/rules/%s/%s", c.rulerAddress, url.PathEscape(namespace), url.PathEscape(groupName)), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer res.Body.Close()
	return nil
}
