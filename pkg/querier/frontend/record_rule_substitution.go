package frontend

import (
	"context"
	"github.com/prometheus/prometheus/promql"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/weaveworks/common/user"
	yaml "gopkg.in/yaml.v2"
)

var (
	// This offset is added to the modifiedAtMillis to give room to the first evaluation
	// of all recording rules.
	safetyOffset = int64(5 * time.Minute / time.Millisecond)
)

func newRecordRuleSubstitutionMiddleware(filename string) (queryRangeMiddleware, *OrgToQueryRecordingRulesMap, error) {
	qrrMap := &OrgToQueryRecordingRulesMap{}
	if filename == "" {
		return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
			return recordRuleSubstitution{
				next:   next,
				qrrMap: qrrMap,
			}
		}), qrrMap, nil
	}

	err := qrrMap.LoadFromFile(filename)
	if err != nil {
		return nil, nil, err
	}

	return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
		return recordRuleSubstitution{
			next:   next,
			qrrMap: qrrMap,
		}
	}), qrrMap, nil
}

type recordRuleSubstitution struct {
	next   queryRangeHandler
	qrrMap *OrgToQueryRecordingRulesMap
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

// OrgToQueryRecordingRulesMap gives thread safe access to
// query to recording rule map of all organisations.
type OrgToQueryRecordingRulesMap struct {
	qrrMap map[string][]QueryToRecordingRuleMap
	mtx    sync.RWMutex
}

// LoadFromFile clears the previous config and loads the query to recording rule
// config from a file.
func (oqr *OrgToQueryRecordingRulesMap) LoadFromFile(filename string) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return oqr.LoadFromBytes(b)
}

// LoadFromBytes clears the previous config and loads the query to recording rule
// config from bytes.
func (oqr *OrgToQueryRecordingRulesMap) LoadFromBytes(b []byte) error {
	oqr.mtx.Lock()
	defer oqr.mtx.Unlock()

	var parsedFile queryToRecordingRuleFile
	var err error
	if err = yaml.UnmarshalStrict(b, &parsedFile); err != nil {
		return err
	}

	// Creating the query to recording rule map from the file.
	oqr.qrrMap = make(map[string][]QueryToRecordingRuleMap)
	for _, o := range parsedFile.Orgs {
		for i := range o.QueryToRuleMap {
			o.QueryToRuleMap[i].Query, err = generaliseQuery(o.QueryToRuleMap[i].Query)
			if err != nil {
				return err
			}
		}
		oqr.qrrMap[o.OrgID] = append(oqr.qrrMap[o.OrgID], o.QueryToRuleMap...)
	}
	return nil
}

// GetMapsForOrg returns the query to recording rule map for the given organisation.
func (oqr *OrgToQueryRecordingRulesMap) GetMapsForOrg(org string) []QueryToRecordingRuleMap {
	oqr.mtx.RLock()
	defer oqr.mtx.RUnlock()
	return oqr.qrrMap[org]
}

// ReplaceQueryWithRecordingRule returns *QueryRangeRequest with the parts of query replaced
// with recording rules according to the query to recording rule map that it has.
// It will replace the query if its recording rules was modified before r.End,
// and query request ranges should be handled separately for returned QueryRangeRequest.
func (oqr *OrgToQueryRecordingRulesMap) ReplaceQueryWithRecordingRule(org string, r *QueryRangeRequest) (*QueryRangeRequest, time.Time, error) {
	qrrs := oqr.GetMapsForOrg(org)
	if len(qrrs) == 0 {
		return nil, time.Unix(0, 0), nil
	}

	rReplaced := *r
	if q, err := generaliseQuery(r.Query); err != nil {
		return nil, time.Unix(0, 0), err
	} else {
		rReplaced.Query = q
	}

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
func (oqr *OrgToQueryRecordingRulesMap) GetMatchingRules(org, query string) ([]QueryToRecordingRuleMap, error) {
	result := []QueryToRecordingRuleMap{}

	qrrs := oqr.GetMapsForOrg(org)
	if len(qrrs) == 0 {
		return result, nil
	}

	query, err := generaliseQuery(query)
	if err != nil {
		return nil, err
	}

	for _, qrr := range qrrs {
		if strings.Contains(query, qrr.Query) {
			result = append(result, qrr)
		}
	}

	return result, nil
}

// structs for the file containing query to recording rule map.

// queryToRecordingRuleFile is the top most level in the file.
type queryToRecordingRuleFile struct {
	Orgs []orgQueryToRecordingRuleMap `yaml:"orgs"`
}

// orgQueryToRecordingRuleMap holds all the recording rules of an organisation.
type orgQueryToRecordingRuleMap struct {
	OrgID          string                    `yaml:"org_id"`
	QueryToRuleMap []QueryToRecordingRuleMap `yaml:"rules"`
}

// QueryToRecordingRuleMap is a single query to recording rule map.
type QueryToRecordingRuleMap struct {
	RuleName   string    `yaml:"name"`
	Query      string    `yaml:"query"`
	ModifiedAt time.Time `yaml:"modifiedAt"`
}
