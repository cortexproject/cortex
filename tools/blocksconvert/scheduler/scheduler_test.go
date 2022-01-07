package scheduler

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/tools/blocksconvert"
)

func TestScanForPlans(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/1.plan", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/1.starting.1234567", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/1.inprogress.2345678", strings.NewReader("")))

	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/2.plan", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/2.inprogress.93485345", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/2.finished.01E8GCW9J0HV0992HSZ0N6RAMN", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/2.finished.01EE9Y140JP4T58X8FGTG5T17F", strings.NewReader("")))

	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/3.plan", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/3.error", strings.NewReader("")))

	// Only error, progress or finished
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/4.error", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/5.progress.1234234", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/12345/6.finished.cleaned", strings.NewReader("")))

	plans, err := scanForPlans(context.Background(), bucket, "migration/12345/")
	require.NoError(t, err)

	require.Equal(t, map[string]plan{
		"1": {
			PlanFiles: []string{"migration/12345/1.plan"},
			ProgressFiles: map[string]time.Time{
				"migration/12345/1.starting.1234567":   time.Unix(1234567, 0),
				"migration/12345/1.inprogress.2345678": time.Unix(2345678, 0),
			},
		},
		"2": {
			PlanFiles: []string{"migration/12345/2.plan"},
			ProgressFiles: map[string]time.Time{
				"migration/12345/2.inprogress.93485345": time.Unix(93485345, 0),
			},
			Finished: []string{"01E8GCW9J0HV0992HSZ0N6RAMN", "01EE9Y140JP4T58X8FGTG5T17F"},
		},
		"3": {
			PlanFiles: []string{"migration/12345/3.plan"},
			ErrorFile: "migration/12345/3.error",
		},
		"4": {
			ErrorFile: "migration/12345/4.error",
		},
		"5": {
			ProgressFiles: map[string]time.Time{
				"migration/12345/5.progress.1234234": time.Unix(1234234, 0),
			},
		},
		"6": {
			Finished: []string{"cleaned"},
		},
	}, plans)
}

func TestSchedulerScan(t *testing.T) {
	now := time.Now()
	nowMinus1Hour := now.Add(-time.Hour)

	bucket := objstore.NewInMemBucket()
	require.NoError(t, bucket.Upload(context.Background(), "migration/user1/1.plan", strings.NewReader("")))
	// This progress file is too old, will be removed.
	require.NoError(t, bucket.Upload(context.Background(), fmt.Sprintf("migration/user1/1.inprogress.%d", nowMinus1Hour.Unix()), strings.NewReader("")))

	require.NoError(t, bucket.Upload(context.Background(), "migration/user2/2.plan", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), fmt.Sprintf("migration/user2/2.inprogress.%d", now.Unix()), strings.NewReader("")))

	require.NoError(t, bucket.Upload(context.Background(), "migration/user3/3.plan", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/user3/3.error", strings.NewReader("")))

	require.NoError(t, bucket.Upload(context.Background(), "migration/user4/4.plan", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/user4/5.error", strings.NewReader("")))
	require.NoError(t, bucket.Upload(context.Background(), "migration/user4/6.finished.01E8GCW9J0HV0992HSZ0N6RAMN", strings.NewReader("")))

	require.NoError(t, bucket.Upload(context.Background(), "migration/ignoredUser/7.plan", strings.NewReader("")))

	ignoredUsers := regexp.MustCompile("ignored.*")

	s := newSchedulerWithBucket(log.NewLogfmtLogger(os.Stdout), bucket, "migration", blocksconvert.AllowAllUsers, ignoredUsers, Config{
		ScanInterval:        10 * time.Second,
		PlanScanConcurrency: 5,
		MaxProgressFileAge:  5 * time.Minute,
	}, nil)

	require.NoError(t, s.scanBucketForPlans(context.Background()))
	require.Equal(t, []queuedPlan{
		{DayIndex: 4, PlanFile: "migration/user4/4.plan"},
	}, s.plansQueue)
	require.Equal(t, "migration/user1/1.error", s.allUserPlans["user1"]["1"].ErrorFile)

	{
		p, pg := s.nextPlanNoRunningCheck(context.Background())
		require.Equal(t, "migration/user4/4.plan", p)
		ok, err := bucket.Exists(context.Background(), pg)
		require.NoError(t, err)
		require.True(t, ok)
	}

	{
		p, pg := s.nextPlanNoRunningCheck(context.Background())
		require.Equal(t, "", p)
		require.Equal(t, "", pg)
	}
}
