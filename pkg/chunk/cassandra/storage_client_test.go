package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestConfig_setClusterConfig_noAuth(t *testing.T) {
	cqlCfg := gocql.NewCluster()
	cfg := Config{
		Auth: false,
	}
	require.NoError(t, cfg.Validate())

	err := cfg.setClusterConfig(cqlCfg)
	require.NoError(t, err)
	assert.Nil(t, cqlCfg.Authenticator)
}

func TestConfig_setClusterConfig_authWithPassword(t *testing.T) {
	cqlCfg := gocql.NewCluster()
	cfg := Config{
		Auth:     true,
		Username: "user",
		Password: "pass",
	}
	require.NoError(t, cfg.Validate())

	err := cfg.setClusterConfig(cqlCfg)
	require.NoError(t, err)
	assert.NotNil(t, cqlCfg.Authenticator)
	assert.Equal(t, "user", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Username)
	assert.Equal(t, "pass", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Password)
}

func TestConfig_setClusterConfig_authWithPasswordFile_noTailLn(t *testing.T) {
	cqlCfg := gocql.NewCluster()
	cfg := Config{
		Auth:         true,
		Username:     "user",
		PasswordFile: "testdata/pass1.txt",
	}
	require.NoError(t, cfg.Validate())

	err := cfg.setClusterConfig(cqlCfg)
	require.NoError(t, err)
	assert.NotNil(t, cqlCfg.Authenticator)
	assert.Equal(t, "user", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Username)
	assert.Equal(t, "pass", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Password)
}

func TestConfig_setClusterConfig_authWithPasswordFile_tailLn(t *testing.T) {
	cqlCfg := gocql.NewCluster()
	cfg := Config{
		Auth:         true,
		Username:     "user",
		PasswordFile: "testdata/pass2.txt",
	}
	require.NoError(t, cfg.Validate())

	err := cfg.setClusterConfig(cqlCfg)
	require.NoError(t, err)
	assert.NotNil(t, cqlCfg.Authenticator)
	assert.Equal(t, "user", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Username)
	assert.Equal(t, "pass", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Password)
}

func TestConfig_setClusterConfig_authWithPasswordAndPasswordFile(t *testing.T) {
	cfg := Config{
		Auth:         true,
		Username:     "user",
		Password:     "pass",
		PasswordFile: "testdata/pass2.txt",
	}
	assert.Error(t, cfg.Validate())
}
