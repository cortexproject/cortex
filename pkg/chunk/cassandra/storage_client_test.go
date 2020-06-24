package cassandra

import (
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/flagext"
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
		Password: flagext.Secret{Value: "pass"},
	}
	require.NoError(t, cfg.Validate())

	err := cfg.setClusterConfig(cqlCfg)
	require.NoError(t, err)
	assert.NotNil(t, cqlCfg.Authenticator)
	assert.Equal(t, "user", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Username)
	assert.Equal(t, "pass", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Password)
}

func TestConfig_setClusterConfig_authWithPasswordFile_withoutTrailingNewline(t *testing.T) {
	cqlCfg := gocql.NewCluster()
	cfg := Config{
		Auth:         true,
		Username:     "user",
		PasswordFile: "testdata/password-without-trailing-newline.txt",
	}
	require.NoError(t, cfg.Validate())

	err := cfg.setClusterConfig(cqlCfg)
	require.NoError(t, err)
	assert.NotNil(t, cqlCfg.Authenticator)
	assert.Equal(t, "user", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Username)
	assert.Equal(t, "pass", cqlCfg.Authenticator.(gocql.PasswordAuthenticator).Password)
}

func TestConfig_setClusterConfig_authWithPasswordFile_withTrailingNewline(t *testing.T) {
	cqlCfg := gocql.NewCluster()
	cfg := Config{
		Auth:         true,
		Username:     "user",
		PasswordFile: "testdata/password-with-trailing-newline.txt",
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
		Password:     flagext.Secret{Value: "pass"},
		PasswordFile: "testdata/password-with-trailing-newline.txt",
	}
	assert.Error(t, cfg.Validate())
}

type sessionDetails struct {
	name, purpose string
}

func TestConfig_Session(t *testing.T) {
	addresses := os.Getenv("CASSANDRA_TEST_ADDRESSES")
	if addresses == "" {
		return
	}

	var cfg Config
	flagext.DefaultValues(&cfg)
	cfg.Addresses = addresses
	cfg.Keyspace = "test"
	cfg.Consistency = "QUORUM"
	cfg.ReplicationFactor = 1

	for _, tc := range []struct {
		name            string
		sessionDetails1 sessionDetails
		sessionDetails2 sessionDetails
		panicsWithErr   string
	}{
		{
			name: "same name and purpose",
			sessionDetails1: sessionDetails{
				name:    "session-test",
				purpose: "foo",
			},
			sessionDetails2: sessionDetails{
				name:    "session-test",
				purpose: "foo",
			},
			panicsWithErr: "duplicate metrics collector registration attempted",
		},
		{
			name: "same name and different purposes",
			sessionDetails1: sessionDetails{
				name:    "session-test",
				purpose: "foo",
			},
			sessionDetails2: sessionDetails{
				name:    "session-test",
				purpose: "bar",
			},
		},
		{
			name: "same name and empty purpose",
			sessionDetails1: sessionDetails{
				name: "session-test",
			},
			sessionDetails2: sessionDetails{
				name: "session-test",
			},
			panicsWithErr: "duplicate metrics collector registration attempted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registerer := prometheus.NewRegistry()

			session1, err := cfg.session(tc.sessionDetails1.name, tc.sessionDetails1.purpose, registerer)
			require.NoError(t, err)

			defer func() {
				session1.Close()
			}()

			if tc.panicsWithErr != "" {
				require.PanicsWithError(t, tc.panicsWithErr, func() {
					_, _ = cfg.session(tc.sessionDetails2.name, tc.sessionDetails2.purpose, registerer)
				})
			} else {
				session2, err := cfg.session(tc.sessionDetails2.name, tc.sessionDetails2.purpose, registerer)
				require.NoError(t, err)

				defer func() {
					session2.Close()
				}()
			}
		})
	}
}
