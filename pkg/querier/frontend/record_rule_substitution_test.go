package frontend

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

var fileInputs = []string{
	`
orgs:
  org1: 
    - name: sum_rate_some_metric_1d
      query: "sum(rate(some_metric[1d]))"
      modifiedAt: 1970-01-01T00:33:20+00:00 # 2000s
    - name: sum_rate_some_metric_5d
      query: "sum(rate(some_metric[5d]))"
      modifiedAt: 1970-01-01T02:46:40+00:00 # 10000s
  org2: 
    - name: prefix:sum_rate_some_metric_1d
      query: "sum(rate(some_metric[1d]))"
      modifiedAt: 1970-01-01T00:33:20+00:00 # 2000s
    - name: prefix:sum_rate_some_metric_5d
      query: "sum(rate(some_metric[5d]))"
      modifiedAt: 1970-01-01T02:46:40+00:00 # 10000s
`,
	`
orgs:
  org1: 
    - name: sum_rate_some_metric_1d
      query: "sum(rate(some_metric[1d]))"
      modifiedAt: 1970-01-19T00:33:20+00:00
    - name: sum_rate_some_metric_5d
      query: "sum(rate(some_metric[5d]))"
      modifiedAt: 1970-01-15T02:46:40+00:00
`,
	`
orgs:
  org2: 
    - name: prefix:sum_rate_some_metric_1d
      query: "sum(rate(some_metric[1d]))"
      modifiedAt: 1977-01-11T05:33:20+00:00
    - name: prefix:sum_rate_some_metric_5d
      query: "sum(rate(some_metric[5d]))"
      modifiedAt: 1977-01-15T11:43:40+00:00
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
		qrrMap := RecordRuleSubstitutionConfig{}
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
						{
							Start: 1000 * 1000,
							End:   (2000 * 1000) + safetyOffset,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d])))",
						},
						{
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
						{
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
						{
							Start: 1000 * 1000,
							End:   (2000 * 1000) + safetyOffset,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
						},
						{
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
						{
							Start: 1000 * 1000,
							End:   (10000 * 1000) + safetyOffset,
							Step:  10 * 1000,
							Query: "avg(sum(rate(some_metric[1d]))) + avg(sum(rate(some_metric[5d])))",
						},
						{
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
						{
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

	rr := recordRuleSubstitution{}
	rr.qrrMap = &RecordRuleSubstitutionConfig{}
	for _, tst := range tests {
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
						{
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
						{
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
						{
							RuleName:   "sum_rate_some_metric_1d",
							Query:      "sum(rate(some_metric[1d]))",
							ModifiedAt: mustParseTime("1970-01-01T00:33:20+00:00"),
						},
						{
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
		qrrMap := &RecordRuleSubstitutionConfig{}
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

func TestWatchConfigFile(t *testing.T) {
	configReloadInterval = 500 * time.Millisecond
	filename := "test_config.yml"
	defer func() {
		os.Remove(filename)
	}()

	// Initial config.
	require.NoError(t, ioutil.WriteFile(filename, []byte(fileInputs[0]), 0644))

	qrm, reloader, err := newRecordRuleSubstitutionMiddleware(filename, log.NewNopLogger())
	require.NoError(t, err)

	testExpected := func(configContent string) {
		t.Helper()
		<-time.After(1000 * time.Millisecond) // So that the reloader can update.

		rrs, ok := qrm.Wrap(nil).(*recordRuleSubstitution)
		require.True(t, ok)

		var qrrMapExp RecordRuleSubstitutionConfig
		err := qrrMapExp.LoadFromBytes([]byte(configContent))
		require.NoError(t, err)

		// Location (memory)pointer differs sometimes, so normalising it.
		qrrMapExp.mtx.Lock()
		rrs.qrrMap.mtx.Lock()
		for _, v := range qrrMapExp.QrrMap {
			for i, q := range v {
				v[i].ModifiedAt = q.ModifiedAt.In(time.UTC)
			}
		}
		for _, v := range rrs.qrrMap.QrrMap {
			for i, q := range v {
				v[i].ModifiedAt = q.ModifiedAt.In(time.UTC)
			}
		}

		require.Equal(t, qrrMapExp.QrrMap, rrs.qrrMap.QrrMap)
		qrrMapExp.mtx.Unlock()
		rrs.qrrMap.mtx.Unlock()
	}

	// Test the normal load.
	testExpected(fileInputs[0])

	// Modify the file.
	require.NoError(t, ioutil.WriteFile(filename, []byte(fileInputs[1]), 0644))
	testExpected(fileInputs[1])

	// Modify the file.
	require.NoError(t, ioutil.WriteFile(filename, []byte(fileInputs[2]), 0644))
	testExpected(fileInputs[2])

	// Delete the file. Watcher doesn't update the config here.
	require.NoError(t, os.Remove(filename))
	testExpected(fileInputs[2])

	// New file.
	require.NoError(t, ioutil.WriteFile(filename, []byte(fileInputs[1]), 0644))
	testExpected(fileInputs[1])

	reloader.Stop()
}
