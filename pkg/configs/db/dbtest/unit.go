// +build !integration

package dbtest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/cortex/pkg/configs/db"
)

// Setup sets up stuff for testing, creating a new database
func Setup(t *testing.T) db.DB {
	require.NoError(t, logging.Setup("debug"))
	database, err := db.New(db.Config{
		URI: "memory://",
	})
	require.NoError(t, err)
	return database
}

// Cleanup cleans up after a test
func Cleanup(t *testing.T, database db.DB) {
	require.NoError(t, database.Close())
}
