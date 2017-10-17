package util

import (
	"flag"
	"fmt"

	"github.com/prometheus/common/log"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

type levelFlag string

// String implements flag.Value.
func (f levelFlag) String() string {
	return fmt.Sprintf("%q", string(f))
}

// Set implements flag.Value.
func (f levelFlag) Set(level string) error {
	return log.Base().SetLevel(level)
}

// LogLevel allows registering flags for configuring the global prometheus/common/log logger.
type LogLevel struct{}

// RegisterFlags adds the flags required to configure the global prometheus/common/log log level.
func (l LogLevel) RegisterFlags(f *flag.FlagSet) {
	f.Var(
		levelFlag("info"),
		"log.level",
		"Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal]",
	)
}

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
