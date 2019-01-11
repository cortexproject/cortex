package frontend

import (
	"context"
	"errors"
	"github.com/cortexproject/cortex/pkg/util"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/user"
	yaml "gopkg.in/yaml.v2"
)

var (
	// This offset is added to the modifiedAtMillis to give room to the first evaluation
	// of all recording rules.
	safetyOffset = int64(5 * time.Minute / time.Millisecond)

	configReloadInterval = 30 * time.Second
)

func newRecordRuleSubstitutionMiddleware(filename string, logger log.Logger) (queryRangeMiddleware, *util.Reloader, error) {
	if filename == "" {
		return nil, nil, errors.New("config file is missing")
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}

	qrrMap := &RecordRuleSubstitutionConfig{}
	err := qrrMap.LoadFromFile(filename)
	if err != nil {
		return nil, nil, err
	}

	reloader, err := util.NewReloader(filename, configReloadInterval, configReloadCallback(qrrMap, logger))
	if err != nil {
		return nil, nil, err
	}

	return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
		return &recordRuleSubstitution{
			next:           next,
			qrrMap:         qrrMap,
			configFilename: filename,
		}
	}), reloader, nil
}

func configReloadCallback(qrrMap *RecordRuleSubstitutionConfig, logger log.Logger) func(f *os.File, err error) {
	return func(f *os.File, err error) {
		if b, err := ioutil.ReadAll(f); err != nil {
			level.Error(logger).Log("msg", "reading file failed in configReloadCallback", "err", err.Error())
		} else if err := qrrMap.LoadFromBytes(b); err != nil {
			level.Error(logger).Log("msg", "failed to load config in configReloadCallback", "err", err.Error())
		}
	}
}

type recordRuleSubstitution struct {
	next   queryRangeHandler
	qrrMap *RecordRuleSubstitutionConfig

	configFilename string
}

func (rr recordRuleSubstitution) Do(ctx context.Context, r *QueryRangeRequest) (*APIResponse, error) {
	reqs, err := rr.replaceQueryWithRecordingRule(ctx, r)
	if err != nil {
		return nil, err
	}

	reqResps, err := doRequests(ctx, rr.next, reqs)
	if err != nil {
		return nil, err
	}

	resps := make([]*APIResponse, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.resp)
	}

	return mergeAPIResponses(resps)
}

func (rr recordRuleSubstitution) replaceQueryWithRecordingRule(ctx context.Context, r *QueryRangeRequest) ([]*QueryRangeRequest, error) {
	if rr.qrrMap == nil {
		return []*QueryRangeRequest{r}, nil
	}

	org, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	rReplaced, maxModifiedAt, err := rr.qrrMap.ReplaceQueryWithRecordingRule(org, r)
	if err != nil {
		return nil, err
	}

	if rReplaced == nil {
		return []*QueryRangeRequest{r}, nil
	}

	modifiedAtMillis := maxModifiedAt.UnixNano() / int64(time.Millisecond/time.Nanosecond)
	modifiedAtMillis = (modifiedAtMillis / r.Step) * r.Step
	if modifiedAtMillis > r.End {
		// No recording rule exist for the query range.
		return []*QueryRangeRequest{r}, nil
	}
	if modifiedAtMillis <= r.Start {
		// Recording rule exists for entire query range.
		return []*QueryRangeRequest{rReplaced}, nil
	}

	// Recording rule exists for partial query range.

	alignedSafetyOffset := (safetyOffset / r.Step) * r.Step
	r.End = modifiedAtMillis + alignedSafetyOffset

	rReplaced.Start = r.End + r.Step
	if rReplaced.Start >= rReplaced.End {
		// Very less evaulations using the recording rule.
		// This is also possible because of adding 'alignedSafetyOffset'.
		return []*QueryRangeRequest{r}, nil
	}

	return []*QueryRangeRequest{r, rReplaced}, nil
}

// generaliseQuery returns the query by formatting it with PromQL printer.
// This will generalise the query structure, and also check for errors in query.
func generaliseQuery(q string) (string, error) {
	expr, err := promql.ParseExpr(q)
	if err != nil {
		return "", err
	}
	return expr.String(), nil
}

// RecordRuleSubstitutionConfig gives thread safe access to
// query to recording rule map of all organisations.
type RecordRuleSubstitutionConfig struct {
	QrrMap map[string][]QueryToRecordingRuleMap `yaml:"orgs"` // (org id) -> its query-recording rule maps.
	mtx    sync.RWMutex
}

// QueryToRecordingRuleMap is a single query to recording rule map.
type QueryToRecordingRuleMap struct {
	RuleName   string    `yaml:"name"`
	Query      string    `yaml:"query"`
	ModifiedAt time.Time `yaml:"modifiedAt"`
}

// LoadFromFile clears the previous config and loads the query to recording rule
// config from a file.
func (rrs *RecordRuleSubstitutionConfig) LoadFromFile(filename string) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		rrs.QrrMap = nil
		return err
	}
	return rrs.LoadFromBytes(b)
}

// LoadFromBytes clears the previous config and loads the query to recording rule
// config from bytes.
func (rrs *RecordRuleSubstitutionConfig) LoadFromBytes(b []byte) (err error) {
	rrs.mtx.Lock()
	defer func() {
		if err != nil {
			rrs.QrrMap = nil
		}
		rrs.mtx.Unlock()
	}()

	var tempRrs RecordRuleSubstitutionConfig
	if err = yaml.UnmarshalStrict(b, &tempRrs); err != nil {
		return err
	}
	rrs.QrrMap = tempRrs.QrrMap

	// Sanitize the query.
	for _, org := range rrs.QrrMap {
		for i := range org {
			org[i].Query, err = generaliseQuery(org[i].Query)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// GetMapsForOrg returns the query to recording rule map for the given organisation.
func (rrs *RecordRuleSubstitutionConfig) GetMapsForOrg(org string) []QueryToRecordingRuleMap {
	rrs.mtx.RLock()
	defer rrs.mtx.RUnlock()
	if rrs.QrrMap == nil {
		return nil
	}
	return rrs.QrrMap[org]
}

// ReplaceQueryWithRecordingRule returns *QueryRangeRequest with the parts of query replaced
// with recording rules according to the query to recording rule map that it has.
// It will replace the query if its recording rules was modified before r.End,
// and query request ranges should be handled separately for returned QueryRangeRequest.
func (rrs *RecordRuleSubstitutionConfig) ReplaceQueryWithRecordingRule(org string, r *QueryRangeRequest) (*QueryRangeRequest, time.Time, error) {
	qrrs := rrs.GetMapsForOrg(org)
	if len(qrrs) == 0 {
		return nil, time.Unix(0, 0), nil
	}

	rReplaced := *r
	q, err := generaliseQuery(r.Query)
	if err != nil {
		return nil, time.Unix(0, 0), err
	}
	rReplaced.Query = q

	ruleSubstituted := false
	var maxModifiedAt time.Time
	for _, qrr := range qrrs {
		modifiedAtMillis := qrr.ModifiedAt.UnixNano() / int64(time.Millisecond/time.Nanosecond)
		if modifiedAtMillis > r.End {
			continue
		}
		if strings.Contains(rReplaced.Query, qrr.Query) {
			rReplaced.Query = strings.Replace(rReplaced.Query, qrr.Query, qrr.RuleName, -1)
			if !ruleSubstituted {
				maxModifiedAt = qrr.ModifiedAt
				ruleSubstituted = true
			}
			if qrr.ModifiedAt.Sub(maxModifiedAt) > 0 {
				maxModifiedAt = qrr.ModifiedAt
			}
		}
	}

	if !ruleSubstituted {
		return nil, time.Unix(0, 0), nil
	}

	return &rReplaced, maxModifiedAt, nil
}

// GetMatchingRules returns QueryToRecordingRuleMap for queries that can be replaced with recording rules in given query.
func (rrs *RecordRuleSubstitutionConfig) GetMatchingRules(org, query string) ([]QueryToRecordingRuleMap, error) {
	qrrs := rrs.GetMapsForOrg(org)
	if len(qrrs) == 0 {
		return nil, nil
	}
	query, err := generaliseQuery(query)
	if err != nil {
		return nil, err
	}
	result := []QueryToRecordingRuleMap{}
	for _, qrr := range qrrs {
		if strings.Contains(query, qrr.Query) {
			result = append(result, qrr)
		}
	}
	return result, nil
}
