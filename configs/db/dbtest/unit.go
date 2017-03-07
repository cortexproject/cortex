// +build !integration

package dbtest

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/service/configs/db"
)

var (
	databaseURI        = flag.String("database-uri", "memory://", "Uri of a test database")
	databaseMigrations = flag.String("database-migrations", "", "Path where the database migration files can be found")
)

// Setup sets up stuff for testing, creating a new database
func Setup(t *testing.T) db.DB {
	require.NoError(t, logging.Setup("debug"))
	database := db.MustNew(*databaseURI, *databaseMigrations)
	return database
}

// Cleanup cleans up after a test
func Cleanup(t *testing.T, database db.DB) {
	require.NoError(t, database.Close())
}
