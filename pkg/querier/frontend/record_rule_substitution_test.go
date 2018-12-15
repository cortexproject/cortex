package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

var fileInputs = []string{
	`
orgs:
  - org_id: org1
    rules: 
      - name: sum_rate_some_metric_1d
        query: "sum(rate(some_metric[1d]))"
        modifiedAt: 1970-01-01T00:33:20+00:00 # 2000s
      - name: sum_rate_some_metric_5d
        query: "sum(rate(some_metric[5d]))"
        modifiedAt: 1970-01-01T02:46:40+00:00 # 10000s
  - org_id: org2
    rules: 
      - name: prefix:sum_rate_some_metric_1d
        query: "sum(rate(some_metric[1d]))"
        modifiedAt: 1970-01-01T00:33:20+00:00 # 2000s
      - name: prefix:sum_rate_some_metric_5d
        query: "sum(rate(some_metric[5d]))"
        modifiedAt: 1970-01-01T02:46:40+00:00 # 10000s
    `,
}

func TestReplaceQueryWithRecordingRule(t *testing.T) {
	tests := []struct {
		file      string
		unittests []struct {
			org            string
			inputQuery     *QueryRangeRequest
			expQuery       *QueryRangeRequest
			expMaxActiveAt time.Time
		}
	}{
		{
			file: fileInputs[0],
			unittests: []struct {
				org            string
				inputQuery     *QueryRangeRequest
				expQuery       *QueryRangeRequest
				expMaxActiveAt time.Time
			}{
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum(rate(some_metric[1d])))",
					},
					expQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum_rate_some_metric_1d)",
					},
					expMaxActiveAt: time.Unix(2000, 0).UTC(),
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum(rate(some_metric[5d])))",
					},
					expQuery: nil,
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum(rate(some_metric[5d])))",
					},

					expQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum_rate_some_metric_5d)",
					},
					expMaxActiveAt: time.Unix(10000, 0).UTC(),
				},
				{
					org: "org2",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum(rate(some_metric[1d])))",
					},
					expQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(prefix:sum_rate_some_metric_1d)",
					},
					expMaxActiveAt: time.Unix(2000, 0).UTC(),
				},
				{
					org: "org2",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum(rate(some_metric[5d])))",
					},

					expQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Query: "avg(prefix:sum_rate_some_metric_5d)",
					},
					expMaxActiveAt: time.Unix(10000, 0).UTC(),
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum_rate_some_metric_1d) + avg(sum(rate(some_metric[5d])))",
					},
					expMaxActiveAt: time.Unix(2000, 0).UTC(),
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum_rate_some_metric_1d) + avg(sum_rate_some_metric_5d)",
					},
					expMaxActiveAt: time.Unix(10000, 0).UTC(),
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 10500 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQuery: &QueryRangeRequest{
						Start: 10500 * 1000,
						End:   11000 * 1000,
						Query: "avg(sum_rate_some_metric_1d) + avg(sum_rate_some_metric_5d)",
					},
					expMaxActiveAt: time.Unix(10000, 0).UTC(),
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Query: "avg(sum(rate(some_diff_metric[5d])))",
					},
					expQuery: nil,
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   1500 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQuery: nil,
				},
			},
		},
	}

	for _, tst := range tests {
		qrrMap := OrgToQueryRecordingRulesMap{}
		qrrMap.LoadFromBytes([]byte(tst.file))

		for _, ut := range tst.unittests {
			actQ, maxma, err := qrrMap.ReplaceQueryWithRecordingRule(ut.org, ut.inputQuery)
			require.Empty(t, err)
			require.Exactly(t, ut.expQuery, actQ)
			if ut.expQuery != nil {
				require.Equal(t, 0, int(ut.expMaxActiveAt.Sub(maxma)))
			}
		}
	}
}

func TestRecordRuleSubstitution_replaceQueryWithRecordingRule(t *testing.T) {
	tests := []struct {
		file      string
		unittests []struct {
			org        string
			inputQuery *QueryRangeRequest
			expQueries []*QueryRangeRequest
		}
	}{
		{
			file: fileInputs[0],
			unittests: []struct {
				org        string
				inputQuery *QueryRangeRequest
				expQueries []*QueryRangeRequest
			}{
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Step:  10 * 1000,
						Query: "avg(sum(rate(some_metric[1d])))",
					},
					expQueries: []*QueryRangeRequest{
						&QueryRangeRequest{
							Start: 1000 * 1000,
							End:   (2000 * 1000) + safetyOffset,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d])))",
						},
						&QueryRangeRequest{
							Start: (2000 * 1000) + safetyOffset + (10 * 1000),
							End:   3000 * 1000,
							Step:  10 * 1000,
							Query: "avg(sum_rate_some_metric_1d)",
						},
					},
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   1500 * 1000,
						Step:  10 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQueries: []*QueryRangeRequest{
						&QueryRangeRequest{
							Start: 1000 * 1000,
							End:   1500 * 1000,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
						},
					},
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   3000 * 1000,
						Step:  10 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQueries: []*QueryRangeRequest{
						&QueryRangeRequest{
							Start: 1000 * 1000,
							End:   (2000 * 1000) + safetyOffset,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
						},
						&QueryRangeRequest{
							Start: (2000 * 1000) + safetyOffset + (10 * 1000),
							End:   3000 * 1000,
							Step:  10 * 1000,
							Query: "avg(sum_rate_some_metric_1d) + avg(sum(rate(some_metric[5d])))",
						},
					},
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 1000 * 1000,
						End:   11000 * 1000,
						Step:  10 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQueries: []*QueryRangeRequest{
						&QueryRangeRequest{
							Start: 1000 * 1000,
							End:   (10000 * 1000) + safetyOffset,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
						},
						&QueryRangeRequest{
							Start: (10000 * 1000) + safetyOffset + (10 * 1000),
							End:   11000 * 1000,
							Step:  10 * 1000,
							Query: "avg(sum_rate_some_metric_1d) + avg(sum_rate_some_metric_5d)",
						},
					},
				},
				{
					org: "org1",
					inputQuery: &QueryRangeRequest{
						Start: 11000 * 1000,
						End:   12000 * 1000,
						Step:  10 * 1000,
						Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					},
					expQueries: []*QueryRangeRequest{
						&QueryRangeRequest{
							Start: 11000 * 1000,
							End:   12000 * 1000,
							Step:  10 * 1000,
							Query: "avg(sum_rate_some_metric_1d) + avg(sum_rate_some_metric_5d)",
						},
					},
				},
			},
		},
	}

	for _, tst := range tests {
		rr := recordRuleSubstitution{}
		rr.qrrMap = &OrgToQueryRecordingRulesMap{}
		rr.qrrMap.LoadFromBytes([]byte(tst.file))

		for _, ut := range tst.unittests {
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, ut.org)

			res, err := rr.replaceQueryWithRecordingRule(ctx, ut.inputQuery)
			require.Empty(t, err)
			require.Exactly(t, ut.expQueries, res)
		}
	}
}

func TestGetMatchingRules(t *testing.T) {
	tests := []struct {
		file      string
		unittests []struct {
			org   string
			query string
			exp   []QueryToRecordingRuleMap
		}
	}{
		{
			file: fileInputs[0],
			unittests: []struct {
				org   string
				query string
				exp   []QueryToRecordingRuleMap
			}{
				{
					org:   "org1",
					query: "avg(sum(rate(some_metric[1d])))",
					exp: []QueryToRecordingRuleMap{
						QueryToRecordingRuleMap{
							RuleName:   "sum_rate_some_metric_1d",
							Query:      "sum(rate(some_metric[1d]))",
							ModifiedAt: mustParseTime("1970-01-01T00:33:20+00:00"),
						},
					},
				},
				{
					org:   "org2",
					query: "avg(sum(rate(some_metric[5d])))",
					exp: []QueryToRecordingRuleMap{
						QueryToRecordingRuleMap{
							RuleName:   "prefix:sum_rate_some_metric_5d",
							Query:      "sum(rate(some_metric[5d]))",
							ModifiedAt: mustParseTime("1970-01-01T02:46:40+00:00"),
						},
					},
				},
				{
					org:   "org1",
					query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
					exp: []QueryToRecordingRuleMap{
						QueryToRecordingRuleMap{
							RuleName:   "sum_rate_some_metric_1d",
							Query:      "sum(rate(some_metric[1d]))",
							ModifiedAt: mustParseTime("1970-01-01T00:33:20+00:00"),
						},
						QueryToRecordingRuleMap{
							RuleName:   "sum_rate_some_metric_5d",
							Query:      "sum(rate(some_metric[5d]))",
							ModifiedAt: mustParseTime("1970-01-01T02:46:40+00:00"),
						},
					},
				},
				{
					org:   "org1",
					query: "avg(sum(irate(some_metric[1d]))) + avg(sum(irate(some_metric[5d])))",
					exp:   []QueryToRecordingRuleMap{},
				},
			},
		},
	}

	for _, tst := range tests {
		qrrMap := &OrgToQueryRecordingRulesMap{}
		qrrMap.LoadFromBytes([]byte(tst.file))

		for _, ut := range tst.unittests {
			act, err := qrrMap.GetMatchingRules(ut.org, ut.query)
			require.Empty(t, err)
			require.Exactly(t, ut.exp, act)
		}
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
