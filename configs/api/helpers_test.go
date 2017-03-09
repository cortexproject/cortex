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

	"github.com/weaveworks/cortex/configs"
	"github.com/weaveworks/cortex/configs/api"
	"github.com/weaveworks/cortex/configs/db"
	"github.com/weaveworks/cortex/configs/db/dbtest"
)

var (
	app      *api.API
	database db.DB
	counter  int
)

// setup sets up the environment for the tests.
func setup(t *testing.T) {
	database = dbtest.Setup(t)
	app = api.New(api.Config{
		OrgIDHeader: api.DefaultOrgIDHeader,
	}, database)
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

// requestAsOrg makes a request to the configs API as the given user.
func requestAsOrg(t *testing.T, userID configs.OrgID, method, urlStr string, body io.Reader) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r, err := http.NewRequest(method, urlStr, body)
	require.NoError(t, err)
	r.Header.Add(app.OrgIDHeader, string(userID))
	app.ServeHTTP(w, r)
	return w
}

// makeString makes a string, guaranteed to be unique within a test.
func makeString(pattern string) string {
	counter++
	return fmt.Sprintf(pattern, counter)
}

// makeOrgID makes an arbitrary organization ID. Guaranteed to be unique within a test.
func makeOrgID() configs.OrgID {
	return configs.OrgID(makeString("org%d"))
}

// makeSubsystem makes an arbitrary name for a subsystem.
func makeSubsystem() configs.Subsystem {
	return configs.Subsystem(makeString("subsystem%d"))
}

// makeConfig makes some arbitrary configuration.
func makeConfig() configs.Config {
	arbitraryKey := makeString("key%d")
	arbitraryValue := makeString("value%d")
	return configs.Config{arbitraryKey: arbitraryValue}
}

type jsonObject map[string]interface{}

func (j jsonObject) Reader(t *testing.T) io.Reader {
	b, err := json.Marshal(j)
	require.NoError(t, err)
	return bytes.NewReader(b)
}

func parseJSON(t *testing.T, b []byte) jsonObject {
	var f jsonObject
	err := json.Unmarshal(b, &f)
	require.NoError(t, err, "Could not unmarshal JSON: %v", string(b))
	return f
}

// parseConfigView parses a ConfigView from JSON.
func parseConfigView(t *testing.T, b []byte) configs.ConfigView {
	var x configs.ConfigView
	err := json.Unmarshal(b, &x)
	require.NoError(t, err, "Could not unmarshal JSON: %v", string(b))
	return x
}
