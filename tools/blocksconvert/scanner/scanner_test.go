package scanner

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

func TestVerifyPlanFile(t *testing.T) {
	testCases := map[string]struct {
		content  string
		errorMsg string
	}{
		"Minimum valid plan file, with no series": {
			content:  `{"user": "test", "day_index": 12345}{"complete": true}`,
			errorMsg: "",
		},
		"no header": {
			content:  `{"complete": true}`,
			errorMsg: "failed to read plan file header: no user or day index found",
		},
		"no footer": {
			content:  `{"user": "test", "day_index": 12345}`,
			errorMsg: "no footer found in the plan",
		},
		"data after footer": {
			content:  `{"user": "test", "day_index": 12345}{"complete": true}{"sid": "some seriesID", "cs": ["chunk1", "chunk2"]}`,
			errorMsg: "plan entries found after plan footer",
		},
		"valid plan with single series": {
			content: `
				{"user": "test", "day_index": 12345}
				{"sid": "some seriesID", "cs": ["chunk1", "chunk2"]}
				{"complete": true}`,
			errorMsg: "",
		},
		"series with no chunks": {
			content: `
				{"user": "test", "day_index": 12345}
				{"sid": "AAAAAA"}
				{"complete": true}`,
			errorMsg: fmt.Sprintf("entry for seriesID %s has no chunks", "AAAAAA"),
		},
		"multiple series entries": {
			content: `
				{"user": "test", "day_index": 12345}
				{"sid": "AAA", "cs": ["chunk1", "chunk2"]}
				{"sid": "AAA", "cs": ["chunk3", "chunk4"]}
				{"complete": true}`,
			errorMsg: "multiple entries for series AAA found in plan",
		},
	}

	for name, tc := range testCases {
		if tc.errorMsg == "" {
			require.NoError(t, verifyPlanFile(strings.NewReader(tc.content)), name)
		} else {
			require.EqualError(t, verifyPlanFile(strings.NewReader(tc.content)), tc.errorMsg, name)
		}
	}
}

func TestVerifyPlansDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "plans")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	of := newOpenFiles(prometheus.NewGauge(prometheus.GaugeOpts{}))
	// This file is checked first, and no error is reported for it.
	require.NoError(t, of.appendJSONEntryToFile(filepath.Join(dir, "user1"), "123.plan", blocksconvert.PlanEntry{User: "user1", DayIndex: 123}, nil))
	require.NoError(t, of.appendJSONEntryToFile(filepath.Join(dir, "user1"), "123.plan", blocksconvert.PlanEntry{SeriesID: "s1", Chunks: []string{"c1, c2"}}, nil))
	require.NoError(t, of.appendJSONEntryToFile(filepath.Join(dir, "user1"), "123.plan", blocksconvert.PlanEntry{Complete: true}, nil))

	require.NoError(t, of.appendJSONEntryToFile(filepath.Join(dir, "user2"), "456.plan", blocksconvert.PlanEntry{User: "user2", DayIndex: 456}, nil))
	require.NoError(t, of.appendJSONEntryToFile(filepath.Join(dir, "user2"), "456.plan", blocksconvert.PlanEntry{SeriesID: "s1", Chunks: []string{"c1, c2"}}, nil))
	require.NoError(t, of.appendJSONEntryToFile(filepath.Join(dir, "user2"), "456.plan", blocksconvert.PlanEntry{SeriesID: "s1", Chunks: []string{"c3, c4"}}, nil))

	require.NoError(t, of.closeAllFiles(nil))

	err = verifyPlanFiles(context.Background(), dir, util.Logger)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "456.plan"))
	require.True(t, strings.Contains(err.Error(), "multiple entries for series s1 found in plan"))
}
