package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/configs/api"
	"github.com/weaveworks/cortex/pkg/configs/db"
	"github.com/weaveworks/cortex/pkg/configs/db/dbtest"
)

var (
	app      *api.API
	database db.DB
	counter  int
)

// setup sets up the environment for the tests.
func setup(t *testing.T) {
	database = dbtest.Setup(t)
	app = api.New(database)
	counter = 0
}

// cleanup cleans up the environment after a test.
func cleanup(t *testing.T) {
	dbtest.Cleanup(t, database)
}

// request makes a request to the configs API.
func request(t *testing.T, method, urlStr string, body io.Reader) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r, err := http.NewRequest(method, urlStr, body)
	require.NoError(t, err)
	app.ServeHTTP(w, r)
	return w
}

// requestAsUser makes a request to the configs API as the given user.
func requestAsUser(t *testing.T, userID string, method, urlStr string, body io.Reader) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r, err := http.NewRequest(method, urlStr, body)
	require.NoError(t, err)
	r = r.WithContext(user.InjectUserID(r.Context(), userID))
	user.InjectUserIDIntoHTTPRequest(r.Context(), r)
	app.ServeHTTP(w, r)
	return w
}

// makeString makes a string, guaranteed to be unique within a test.
func makeString(pattern string) string {
	counter++
	return fmt.Sprintf(pattern, counter)
}

// makeUserID makes an arbitrary user ID. Guaranteed to be unique within a test.
func makeUserID() string {
	return makeString("user%d")
}

// makeConfig makes some arbitrary configuration.
func makeConfig() configs.Config {
	return configs.Config{
		AlertmanagerConfig: makeString(`
            # Config no. %d.
            route:
              receiver: noop

            receivers:
            - name: noop`),
		RulesFiles: nil,
	}
}

func readerFromConfig(t *testing.T, config configs.Config) io.Reader {
	b, err := json.Marshal(config)
	require.NoError(t, err)
	return bytes.NewReader(b)
}

// parseView parses a configs.View from JSON.
func parseView(t *testing.T, b []byte) configs.View {
	var x configs.View
	err := json.Unmarshal(b, &x)
	require.NoError(t, err, "Could not unmarshal JSON: %v", string(b))
	return x
}
