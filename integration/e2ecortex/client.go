package e2ecortex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	open_api_models "github.com/prometheus/alertmanager/api/v2/models"
	alertConfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/types"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	yaml "gopkg.in/yaml.v3"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

var ErrNotFound = errors.New("not found")

var DefaultFilter = RuleFilter{}

// Client is a client used to interact with Cortex in integration tests
type Client struct {
	alertmanagerClient  promapi.Client
	querierAddress      string
	alertmanagerAddress string
	rulerAddress        string
	distributorAddress  string
	timeout             time.Duration
	httpClient          *http.Client
	querierClient       promv1.API
	orgID               string
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
		distributorAddress:  distributorAddress,
		querierAddress:      querierAddress,
		alertmanagerAddress: alertmanagerAddress,
		rulerAddress:        rulerAddress,
		timeout:             5 * time.Second,
		httpClient:          &http.Client{},
		querierClient:       promv1.NewAPI(querierAPIClient),
		orgID:               orgID,
	}

	if alertmanagerAddress != "" {
		alertmanagerAPIClient, err := promapi.NewClient(promapi.Config{
			Address:      "http://" + alertmanagerAddress,
			RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: http.DefaultTransport},
		})
		if err != nil {
			return nil, err
		}
		c.alertmanagerClient = alertmanagerAPIClient
	}

	return c, nil
}

// NewPromQueryClient makes a new client but used for Prometheus Query only.
func NewPromQueryClient(address string) (*Client, error) {
	// Create querier API client
	querierAPIClient, err := promapi.NewClient(promapi.Config{
		Address: "http://" + address,
	})
	if err != nil {
		return nil, err
	}

	c := &Client{
		timeout:       5 * time.Second,
		httpClient:    &http.Client{},
		querierClient: promv1.NewAPI(querierAPIClient),
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

func getNameAndAttributes(ts prompb.TimeSeries) (string, map[string]any) {
	var metricName string
	attributes := make(map[string]any)
	for _, label := range ts.Labels {
		if label.Name == model.MetricNameLabel {
			metricName = label.Value
		} else {
			attributes[label.Name] = label.Value
		}
	}
	return metricName, attributes
}

func createDataPointsGauge(newMetric pmetric.Metric, attributes map[string]any, samples []prompb.Sample) {
	newMetric.SetEmptyGauge()
	for _, sample := range samples {
		datapoint := newMetric.Gauge().DataPoints().AppendEmpty()
		datapoint.SetDoubleValue(sample.Value)
		datapoint.SetTimestamp(pcommon.Timestamp(sample.Timestamp * time.Millisecond.Nanoseconds()))
		err := datapoint.Attributes().FromRaw(attributes)
		if err != nil {
			panic(err)
		}
	}
}

func createDataPointsExponentialHistogram(newMetric pmetric.Metric, attributes map[string]any, histograms []prompb.Histogram) {
	newMetric.SetEmptyExponentialHistogram()
	for _, h := range histograms {
		datapoint := newMetric.ExponentialHistogram().DataPoints().AppendEmpty()
		datapoint.SetTimestamp(pcommon.Timestamp(h.Timestamp * time.Millisecond.Nanoseconds()))
		datapoint.SetCount(h.GetCountInt())
		datapoint.SetSum(h.GetSum())
		datapoint.SetScale(h.GetSchema())
		datapoint.SetZeroCount(h.GetZeroCountInt())
		datapoint.SetZeroThreshold(h.GetZeroThreshold())
		convertBucketLayout(datapoint.Positive(), h.PositiveSpans, h.PositiveDeltas)
		convertBucketLayout(datapoint.Negative(), h.NegativeSpans, h.NegativeDeltas)
		err := datapoint.Attributes().FromRaw(attributes)
		if err != nil {
			panic(err)
		}
	}
}

// convertBucketLayout converts Prometheus remote write bucket layout to Exponential Histogram bucket layout.
func convertBucketLayout(bucket pmetric.ExponentialHistogramDataPointBuckets, spans []prompb.BucketSpan, deltas []int64) {
	vals := make([]uint64, 0)
	iDelta := 0
	count := int64(0)
	for i, span := range spans {
		if i == 0 {
			bucket.SetOffset(span.GetOffset() - 1)
		} else {
			for j := 0; j < int(span.GetOffset()); j++ {
				vals = append(vals, 0)
			}
		}
		for j := 0; j < int(span.Length); j++ {
			count += deltas[iDelta]
			vals = append(vals, uint64(count))
			iDelta++
		}
	}
	bucket.BucketCounts().FromRaw(vals)
}

// Convert Timeseries to Metrics
func convertTimeseriesToMetrics(timeseries []prompb.TimeSeries) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for _, ts := range timeseries {
		metricName, attributes := getNameAndAttributes(ts)
		newMetric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		newMetric.SetName(metricName)
		//TODO Set description for new metric
		//TODO Set unit for new metric
		if len(ts.Samples) > 0 {
			createDataPointsGauge(newMetric, attributes, ts.Samples)
		} else if len(ts.Histograms) > 0 {
			createDataPointsExponentialHistogram(newMetric, attributes, ts.Histograms)
		}
	}
	return metrics
}

// Push series to OTLP endpoint
func (c *Client) OTLP(timeseries []prompb.TimeSeries) (*http.Response, error) {

	data, err := pmetricotlp.NewExportRequestFromMetrics(convertTimeseriesToMetrics(timeseries)).MarshalProto()
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/otlp/v1/metrics", c.distributorAddress), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
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

// Query runs an instant query.
func (c *Client) Query(query string, ts time.Time) (model.Value, error) {
	value, _, err := c.querierClient.Query(context.Background(), query, ts)
	return value, err
}

// QueryRange runs a query range.
func (c *Client) QueryRange(query string, start, end time.Time, step time.Duration) (model.Value, error) {
	value, _, err := c.querierClient.QueryRange(context.Background(), query, promv1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	return value, err
}

// QueryRangeRaw runs a ranged query directly against the querier API.
func (c *Client) QueryRangeRaw(query string, start, end time.Time, step time.Duration, headers map[string]string) (*http.Response, []byte, error) {
	addr := fmt.Sprintf(
		"http://%s/api/prom/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
		c.querierAddress,
		url.QueryEscape(query),
		FormatTime(start),
		FormatTime(end),
		strconv.FormatFloat(step.Seconds(), 'f', -1, 64),
	)

	return c.query(addr, headers)
}

// QueryRaw runs a query directly against the querier API.
func (c *Client) QueryRaw(query string, ts time.Time, headers map[string]string) (*http.Response, []byte, error) {
	u := &url.URL{
		Scheme: "http",
		Path:   fmt.Sprintf("%s/api/prom/api/v1/query", c.querierAddress),
	}
	q := u.Query()
	q.Set("query", query)

	if !ts.IsZero() {
		q.Set("time", FormatTime(ts))
	}
	u.RawQuery = q.Encode()
	return c.query(u.String(), headers)
}

// SeriesRaw runs a series request directly against the querier API.
func (c *Client) SeriesRaw(matches []string, startTime, endTime time.Time, headers map[string]string) (*http.Response, []byte, error) {
	u := &url.URL{
		Scheme: "http",
		Path:   fmt.Sprintf("%s/api/prom/api/v1/series", c.querierAddress),
	}
	q := u.Query()

	for _, m := range matches {
		q.Add("match[]", m)
	}

	if !startTime.IsZero() {
		q.Set("start", FormatTime(startTime))
	}
	if !endTime.IsZero() {
		q.Set("end", FormatTime(endTime))
	}

	u.RawQuery = q.Encode()
	return c.query(u.String(), headers)
}

// LabelNamesRaw runs a label names request directly against the querier API.
func (c *Client) LabelNamesRaw(matches []string, startTime, endTime time.Time, headers map[string]string) (*http.Response, []byte, error) {
	u := &url.URL{
		Scheme: "http",
		Path:   fmt.Sprintf("%s/api/prom/api/v1/labels", c.querierAddress),
	}
	q := u.Query()

	for _, m := range matches {
		q.Add("match[]", m)
	}

	if !startTime.IsZero() {
		q.Set("start", FormatTime(startTime))
	}
	if !endTime.IsZero() {
		q.Set("end", FormatTime(endTime))
	}

	u.RawQuery = q.Encode()
	return c.query(u.String(), headers)
}

// LabelValuesRaw runs a label values request directly against the querier API.
func (c *Client) LabelValuesRaw(label string, matches []string, startTime, endTime time.Time, headers map[string]string) (*http.Response, []byte, error) {
	u := &url.URL{
		Scheme: "http",
		Path:   fmt.Sprintf("%s/api/prom/api/v1/label/%s/values", c.querierAddress, label),
	}
	q := u.Query()

	for _, m := range matches {
		q.Add("match[]", m)
	}

	if !startTime.IsZero() {
		q.Set("start", FormatTime(startTime))
	}
	if !endTime.IsZero() {
		q.Set("end", FormatTime(endTime))
	}

	u.RawQuery = q.Encode()
	return c.query(u.String(), headers)
}

// RemoteRead runs a remote read query.
func (c *Client) RemoteRead(matchers []*labels.Matcher, start, end time.Time, step time.Duration) (*prompb.ReadResponse, error) {
	startMs := start.UnixMilli()
	endMs := end.UnixMilli()
	stepMs := step.Milliseconds()

	q, err := remote.ToQuery(startMs, endMs, matchers, &storage.SelectHints{
		Step:  stepMs,
		Start: startMs,
		End:   endMs,
	})
	if err != nil {
		return nil, err
	}

	req := &prompb.ReadRequest{
		Queries:               []*prompb.Query{q},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	compressed := snappy.Encode(nil, data)

	// Call the remote read API endpoint with a timeout.
	httpReqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(httpReqCtx, "POST", "http://"+c.querierAddress+"/prometheus/api/v1/read", bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("X-Scope-OrgID", "user-1")
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "Prometheus/1.8.2")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", httpResp.StatusCode)
	}

	compressed, err = io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}

	var resp prompb.ReadResponse
	if err = proto.Unmarshal(uncompressed, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) query(addr string, headers map[string]string) (*http.Response, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", addr, nil)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("X-Scope-OrgID", c.orgID)

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 10,
	})
	var (
		res *http.Response
	)
	for retries.Ongoing() {
		res, err = c.httpClient.Do(req)
		if err == nil {
			break
		}
		retries.Wait()
	}
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, err
	}
	return res, body, nil
}

// Series finds series by label matchers.
func (c *Client) Series(matches []string, start, end time.Time) ([]model.LabelSet, error) {
	result, _, err := c.querierClient.Series(context.Background(), matches, start, end)
	return result, err
}

// LabelValues gets label values
func (c *Client) LabelValues(label string, start, end time.Time, matches []string) (model.LabelValues, error) {
	result, _, err := c.querierClient.LabelValues(context.Background(), label, matches, start, end)
	return result, err
}

// LabelNames gets label names
func (c *Client) LabelNames(start, end time.Time, matchers ...string) ([]string, error) {
	result, _, err := c.querierClient.LabelNames(context.Background(), matchers, start, end)
	return result, err
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

type RuleFilter struct {
	Namespaces     []string
	RuleGroupNames []string
	RuleNames      []string
	RuleType       string
	ExcludeAlerts  string
}

func addQueryParams(urlValues url.Values, paramName string, params ...string) {
	for _, paramValue := range params {
		if paramValue != "" {
			urlValues.Add(paramName, paramValue)
		}
	}
}

// GetPrometheusRules fetches the rules from the Prometheus endpoint /api/v1/rules.
func (c *Client) GetPrometheusRules(filter RuleFilter) ([]*ruler.RuleGroup, error) {
	// Create HTTP request

	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/prom/api/v1/rules", c.rulerAddress), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	urlValues := req.URL.Query()
	addQueryParams(urlValues, "file[]", filter.Namespaces...)
	addQueryParams(urlValues, "rule_name[]", filter.RuleNames...)
	addQueryParams(urlValues, "rule_group[]", filter.RuleGroupNames...)
	addQueryParams(urlValues, "type", filter.RuleType)
	addQueryParams(urlValues, "exclude_alerts", filter.ExcludeAlerts)
	req.URL.RawQuery = urlValues.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	// Decode the response.
	type response struct {
		Status string              `json:"status"`
		Data   ruler.RuleDiscovery `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return nil, err
	}

	if decoded.Status != "success" {
		return nil, fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data.RuleGroups, nil
}

// GetRuleGroups gets the configured rule groups from the ruler.
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

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, rgs)
	if err != nil {
		return nil, err
	}

	return rgs, nil
}

// SetRuleGroup configures the provided rulegroup to the ruler.
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

	if res.StatusCode != 202 {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	return nil
}

// GetRuleGroup gets a rule group.
func (c *Client) GetRuleGroup(namespace string, groupName string) (*http.Response, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/prom/rules/%s/%s", c.rulerAddress, url.PathEscape(namespace), url.PathEscape(groupName)), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	return c.httpClient.Do(req.WithContext(ctx))
}

// DeleteRuleGroup deletes a rule group.
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

// DeleteRuleNamespace deletes all the rule groups (and the namespace itself).
func (c *Client) DeleteRuleNamespace(namespace string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/prom/rules/%s", c.rulerAddress, url.PathEscape(namespace)), nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	_, err = c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	return nil
}

// userConfig is used to communicate a users alertmanager configs
type userConfig struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

// GetAlertmanagerStatusPage gets the status page of alertmanager.
func (c *Client) GetAlertmanagerStatusPage(ctx context.Context) ([]byte, error) {
	return c.getRawPage(ctx, "http://"+c.alertmanagerAddress+"/multitenant_alertmanager/status")
}

func (c *Client) getRawPage(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("fetching page failed with status %d and content %v", resp.StatusCode, string(content))
	}
	return content, nil
}

// GetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) GetAlertmanagerConfig(ctx context.Context) (*alertConfig.Config, error) {
	u := c.alertmanagerClient.URL("/api/prom/api/v2/status", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	cfg := &open_api_models.AlertmanagerStatus{}
	err = yaml.Unmarshal(body, cfg)

	if err != nil {
		return nil, err
	}

	original := &alertConfig.Config{}
	err = yaml.Unmarshal([]byte(*cfg.Config.Original), original)

	return original, err
}

// SetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) SetAlertmanagerConfig(ctx context.Context, amConfig string, templates map[string]string) error {
	u := c.alertmanagerClient.URL("/api/v1/alerts", nil)

	data, err := yaml.Marshal(&userConfig{
		AlertmanagerConfig: amConfig,
		TemplateFiles:      templates,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("setting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) DeleteAlertmanagerConfig(ctx context.Context) error {
	u := c.alertmanagerClient.URL("/api/v1/alerts", nil)
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

// SendAlertToAlermanager sends alerts to the Alertmanager API
func (c *Client) SendAlertToAlermanager(ctx context.Context, alert *model.Alert) error {
	u := c.alertmanagerClient.URL("/api/prom/api/v2/alerts", nil)

	data, err := json.Marshal([]types.Alert{{Alert: *alert}})
	if err != nil {
		return fmt.Errorf("error marshaling the alert: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sending alert failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetAlertsV2(ctx context.Context) ([]model.Alert, error) {
	u := c.alertmanagerClient.URL("api/prom/api/v2/alerts", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alerts failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []model.Alert{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

type AlertGroup struct {
	Labels model.LabelSet `json:"labels"`
	Alerts []model.Alert  `json:"alerts"`
}

func (c *Client) GetAlertGroups(ctx context.Context) ([]AlertGroup, error) {
	u := c.alertmanagerClient.URL("api/prom/api/v2/alerts/groups", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alert groups failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []AlertGroup{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

// CreateSilence creates a new silence and returns the unique identifier of the silence.
func (c *Client) CreateSilence(ctx context.Context, silence types.Silence) (string, error) {
	u := c.alertmanagerClient.URL("api/prom/api/v2/silences", nil)

	data, err := json.Marshal(silence)
	if err != nil {
		return "", fmt.Errorf("error marshaling the silence: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("creating the silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		SilenceID string `json:"silenceID"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return "", err
	}
	return decoded.SilenceID, nil
}

func (c *Client) GetSilencesV2(ctx context.Context) ([]types.Silence, error) {
	u := c.alertmanagerClient.URL("api/prom/api/v2/silences", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting silences failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []types.Silence{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

func (c *Client) GetSilenceV2(ctx context.Context, id string) (types.Silence, error) {
	u := c.alertmanagerClient.URL(fmt.Sprintf("api/prom/api/v2/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return types.Silence{}, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return types.Silence{}, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return types.Silence{}, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return types.Silence{}, fmt.Errorf("getting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := types.Silence{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return types.Silence{}, err
	}

	return decoded, nil
}

func (c *Client) DeleteSilence(ctx context.Context, id string) error {
	u := c.alertmanagerClient.URL(fmt.Sprintf("api/prom/api/v2/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetReceivers(ctx context.Context) ([]string, error) {
	u := c.alertmanagerClient.URL("api/prom/api/v2/receivers", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting receivers failed with status %d and error %v", resp.StatusCode, string(body))
	}

	r := []string{}
	type response struct {
		Name string `json:"name"`
	}

	decoded := []response{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	for _, d := range decoded {
		r = append(r, d.Name)
	}

	return r, nil
}

func (c *Client) PostRequest(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Scope-OrgID", c.orgID)

	client := &http.Client{Timeout: c.timeout}
	return client.Do(req)
}

// FormatTime converts a time to a string acceptable by the Prometheus API.
func FormatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}
