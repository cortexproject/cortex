package tenantfederation

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func Test_RegexResolver(t *testing.T) {
	userStat := ingester.UserStats{}

	tests := []struct {
		description     string
		allUserStatFunc func(ctx context.Context) ([]ingester.UserIDStats, error)
		orgID           string
		expectedErr     error
		expectedOrgIDs  []string
	}{
		{
			description: "invalid regex",
			allUserStatFunc: func(ctx context.Context) ([]ingester.UserIDStats, error) {
				return []ingester.UserIDStats{}, nil
			},
			orgID:       "[a-z",
			expectedErr: errInvalidRegex,
		},
		{
			description: "no matched tenantID",
			allUserStatFunc: func(ctx context.Context) ([]ingester.UserIDStats, error) {
				return []ingester.UserIDStats{
					{UserID: "user-1", UserStats: userStat},
					{UserID: "user-2", UserStats: userStat},
					{UserID: "user-3", UserStats: userStat},
				}, nil
			},
			orgID:          "user-[4-6]",
			expectedOrgIDs: []string{"fake"},
		},
		{
			description: "use tenantIDsLabelSeparator",
			allUserStatFunc: func(ctx context.Context) ([]ingester.UserIDStats, error) {
				return []ingester.UserIDStats{
					{UserID: "user-1", UserStats: userStat},
					{UserID: "user-2", UserStats: userStat},
					{UserID: "user-3", UserStats: userStat},
				}, nil
			},
			orgID:          "user-1|user-2|user-3",
			expectedOrgIDs: []string{"user-1", "user-2", "user-3"},
		},
		{
			description: "use regex",
			allUserStatFunc: func(ctx context.Context) ([]ingester.UserIDStats, error) {
				return []ingester.UserIDStats{
					{UserID: "user-1", UserStats: userStat},
					{UserID: "user-2", UserStats: userStat},
					{UserID: "user-3", UserStats: userStat},
				}, nil
			},
			orgID:          "user-.+",
			expectedOrgIDs: []string{"user-1", "user-2", "user-3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			regexResolver := NewRegexResolver(reg, time.Second, log.NewNopLogger(), tc.allUserStatFunc)

			// wait update knownUsers
			test.Poll(t, time.Second*10, true, func() interface{} {
				return testutil.ToFloat64(regexResolver.lastUpdateUserRun) > 0
			})

			// set regexOrgID
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, tc.orgID)
			orgIDs, err := regexResolver.TenantIDs(ctx)

			if tc.expectedErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOrgIDs, orgIDs)
			}
		})
	}
}

func Test_RegexValidator(t *testing.T) {
	tests := []struct {
		description string
		orgID       string
		expectedErr error
	}{
		{
			description: "valid regex",
			orgID:       "user-.*",
			expectedErr: nil,
		},
		{
			description: "invalid regex",
			orgID:       "[a-z",
			expectedErr: errInvalidRegex,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			regexValidator := NewRegexValidator()
			ctx := user.InjectOrgID(context.Background(), tc.orgID)
			_, err := regexValidator.TenantID(ctx)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
