package frontend

import (
	"context"
	"github.com/prometheus/prometheus/promql"
	"io/ioutil"
	"strings"
	"time"

	"github.com/weaveworks/common/user"
	yaml "gopkg.in/yaml.v2"
)

func newRecordRuleSubstitutionMiddleware(filename string) (queryRangeMiddleware, error) {
	if filename == "" {
		return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
			return recordRuleSubstitution{
				next:   next,
				qrrMap: nil,
			}
		}), nil
	}

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var parsedFile queryToRecordingRuleFile
	if err := yaml.UnmarshalStrict(b, &parsedFile); err != nil {
		return nil, err
	}

	qrrMap := make(map[string][]queryToRecordingRuleMap)
	for _, o := range parsedFile.Orgs {
		for i := range o.QueryToRuleMap {
			o.QueryToRuleMap[i].Query, err = generaliseQuery(o.QueryToRuleMap[i].Query)
			if err != nil {
				return nil, err
			}
		}
		qrrMap[o.OrgID] = append(qrrMap[o.OrgID], o.QueryToRuleMap...)
	}

	return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
		return recordRuleSubstitution{
			next:   next,
			qrrMap: qrrMap,
		}
	}), nil
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

type recordRuleSubstitution struct {
	next   queryRangeHandler
	qrrMap map[string][]queryToRecordingRuleMap
}

func (rr recordRuleSubstitution) Do(ctx context.Context, r *QueryRangeRequest) (*APIResponse, error) {
	if rr.qrrMap == nil {
		return rr.next.Do(ctx, r)
	}

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	qrrs, ok := rr.qrrMap[userID]
	if !ok {
		return rr.next.Do(ctx, r)
	}

	r.Query, err = generaliseQuery(r.Query)
	if err != nil {
		return nil, err
	}

	ruleSubstituted := false
	var maxModifiedAt time.Time
	rCopy := &*r
	for _, qrr := range qrrs {
		if strings.Contains(rCopy.Query, qrr.Query) {
			rCopy.Query = strings.Replace(rCopy.Query, qrr.Query, qrr.RuleName, -1)
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
		return rr.next.Do(ctx, r)
	}

	modifiedAtMillis := maxModifiedAt.UnixNano() / int64(time.Millisecond/time.Nanosecond)

	if modifiedAtMillis > r.End {
		return rr.next.Do(ctx, r)
	}
	if modifiedAtMillis < r.Start {
		return rr.next.Do(ctx, rCopy)
	}

	modifiedAtMillis = (modifiedAtMillis / r.Step) * r.Step
	r.End = modifiedAtMillis
	rCopy.Start = r.End + r.Step

	reqResps, err := doRequests(ctx, rr.next, []*QueryRangeRequest{r, rCopy})
	if err != nil {
		return nil, err
	}

	resps := make([]*APIResponse, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.resp)
	}

	return mergeAPIResponses(resps)
}

type queryToRecordingRuleFile struct {
	Orgs []orgQueryToRecordingRuleMap `yaml:"orgs"`
}

type orgQueryToRecordingRuleMap struct {
	OrgID          string                    `yaml:"org_id"`
	QueryToRuleMap []queryToRecordingRuleMap `yaml:"rules"`
}

type queryToRecordingRuleMap struct {
	RuleName   string    `yaml:"name"`
	Query      string    `yaml:"query"`
	ModifiedAt time.Time `yaml:"modifiedAt"`
}
