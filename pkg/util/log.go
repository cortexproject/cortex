package util

import (
	"github.com/prometheus/common/log"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

// WithContext returns a Logger that has information about the current user in
// its details.
//
// e.g.
//   log := util.WithContext(ctx)
//   log.Errorf("Could not chunk chunks: %v", err)
func WithContext(ctx context.Context) log.Logger {
	// Weaveworks uses "orgs" and "orgID" to represent Cortex users,
	// even though the code-base generally uses `userID` to refer to the same thing.
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return log.Base()
	}
	return WithUserID(userID)
}

// WithUserID returns a Logger that has information about the current user in
// its details.
func WithUserID(userID string) log.Logger {
	// See note in WithContext.
	return log.Base().With("orgID", userID)
}
