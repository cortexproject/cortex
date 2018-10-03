// +build integration

package dbtest

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/configs/db/postgres"
	"github.com/weaveworks/common/logging"
)

var (
	done          chan error
	errRollback   = fmt.Errorf("Rolling back test data")
	migrationsDir = os.Getenv("MIGRATIONS_DIR")
)

// Setup sets up stuff for testing, creating a new database
func Setup(t *testing.T) db.DB {
	require.NoError(t, logging.Setup("debug"))
	// Don't use db.MustNew, here so we can do a transaction around the whole test, to rollback.
	pg, err := postgres.New(
		"postgres://postgres@127.0.0.1/configs_test?sslmode=disable",
		migrationsDir,
	)
	require.NoError(t, err)

	newDB := make(chan db.DB)
	done = make(chan error)
	go func() {
		done <- pg.Transaction(func(tx postgres.DB) error {
			// Pass out the tx so we can run the test
			newDB <- tx
			// Wait for the test to finish
			return <-done
		})
	}()
	// Get the new database
	return <-newDB
}

// Cleanup cleans up after a test
func Cleanup(t *testing.T, database db.DB) {
	if done != nil {
		done <- errRollback
		require.Equal(t, errRollback, <-done)
		done = nil
	}
	require.NoError(t, database.Close())
}
