package cortex

import (
	"context"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func changeTargetConfig(c *Config) {
	c.Target = []string{"all", "ruler"}
}

func TestAPIConfig(t *testing.T) {
	actualCfg := newDefaultConfig()

	cortex := &Cortex{
		Server: &server.Server{},
	}

	for _, tc := range []struct {
		name               string
		path               string
		actualCfg          func(*Config)
		expectedStatusCode int
		expectedBody       func(*testing.T, string)
	}{
		{
			name:               "running with default config",
			path:               "/config",
			expectedStatusCode: 200,
		},
		{
			name:               "defaults with default config",
			path:               "/config?mode=defaults",
			expectedStatusCode: 200,
		},
		{
			name:               "diff with default config",
			path:               "/config?mode=diff",
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Equal(t, "{}\n", body)
			},
		},
		{
			name:               "running with changed target config",
			path:               "/config",
			actualCfg:          changeTargetConfig,
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Contains(t, body, "target: all,ruler\n")
			},
		},
		{
			name:               "defaults with changed target config",
			path:               "/config?mode=defaults",
			actualCfg:          changeTargetConfig,
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Contains(t, body, "target: all\n")
			},
		},
		{
			name:               "diff with changed target config",
			path:               "/config?mode=diff",
			actualCfg:          changeTargetConfig,
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Equal(t, "target: all,ruler\n", body)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cortex.Server.HTTP = mux.NewRouter()

			cortex.Cfg = *actualCfg
			if tc.actualCfg != nil {
				tc.actualCfg(&cortex.Cfg)
			}

			_, err := cortex.initAPI()
			require.NoError(t, err)

			req := httptest.NewRequest("GET", tc.path, nil)
			resp := httptest.NewRecorder()

			cortex.Server.HTTP.ServeHTTP(resp, req)

			assert.Equal(t, tc.expectedStatusCode, resp.Code)

			if tc.expectedBody != nil {
				tc.expectedBody(t, resp.Body.String())
			}
		})
	}
}

func TestCortex_InitRulerStorage(t *testing.T) {
	tests := map[string]struct {
		config       *Config
		expectedInit bool
	}{
		"should init the ruler storage with target=ruler": {
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"ruler"}
				cfg.RulerStorage.Backend = "local"
				cfg.RulerStorage.Local.Directory = os.TempDir()
				return cfg
			}(),
			expectedInit: true,
		},
		"should not init the ruler storage on default config with target=all": {
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"all"}
				return cfg
			}(),
			expectedInit: false,
		},
		"should init the ruler storage on ruler storage config with target=all": {
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"all"}
				cfg.RulerStorage.Backend = "local"
				cfg.RulerStorage.Local.Directory = os.TempDir()
				return cfg
			}(),
			expectedInit: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cortex := &Cortex{
				Server: &server.Server{},
				Cfg:    *testData.config,
			}

			_, err := cortex.initRulerStorage()
			require.NoError(t, err)

			if testData.expectedInit {
				assert.NotNil(t, cortex.RulerStorage)
			} else {
				assert.Nil(t, cortex.RulerStorage)
			}
		})
	}
}

type myPusher struct{}

func (p *myPusher) Push(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	return nil, nil
}

type myQueryable struct{}

func (q *myQueryable) Querier(mint, maxt int64) (prom_storage.Querier, error) {
	return prom_storage.NoopQuerier(), nil
}

func Test_setupModuleManager(t *testing.T) {
	tests := []struct {
		config           *Config
		expectedOriginal bool
	}{
		{
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"all"}
				cfg.RulerStorage.Backend = "local"
				cfg.RulerStorage.Local.Directory = os.TempDir()
				cfg.ExternalPusher = &myPusher{}
				cfg.ExternalQueryable = &myQueryable{}
				return cfg
			}(),
			expectedOriginal: false,
		},
		{
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"all"}
				cfg.RulerStorage.Backend = "local"
				cfg.RulerStorage.Local.Directory = os.TempDir()
				return cfg
			}(),
			expectedOriginal: true,
		},
	}

	for _, test := range tests {
		cortex := &Cortex{
			Cfg:    *test.config,
			Server: &server.Server{},
		}

		err := cortex.setupModuleManager()
		require.Nil(t, err)

		deps := cortex.ModuleManager.DependenciesForModule(Ruler)
		originalDependecies := []string{DistributorService, StoreQueryable}

		if test.expectedOriginal {
			check := []bool{false, false}
			for _, dep := range deps {
				for i, o := range originalDependecies {
					if dep == o {
						check[i] = true
					}
				}
			}
			for _, val := range check {
				require.True(t, val)
			}
		} else {
			for _, dep := range deps {
				for _, o := range originalDependecies {
					require.NotEqual(t, dep, o)
				}
			}
		}
	}
}

func Test_initResourceMonitor_shouldFailOnInvalidResource(t *testing.T) {
	cortex := &Cortex{
		Server: &server.Server{},
		Cfg: Config{
			ResourceMonitor: configs.ResourceMonitor{
				Resources: []string{"invalid"},
			},
		},
	}

	// log warning message and spin up other cortex services
	_, err := cortex.initResourceMonitor()
	require.ErrorContains(t, err, "unknown resource type")
}

func TestConfigEndpoint_SecretsMasked(t *testing.T) {
	cfg := newDefaultConfig()

	// Use reflection to find every flagext.Secret field in the config and set
	// it to a sentinel value. This ensures newly added Secret fields are
	// automatically covered without updating this test.
	sentinel := "LEAKED_SECRET_VALUE"
	setAllSecrets(reflect.ValueOf(cfg), sentinel)

	cortex := &Cortex{
		Server: &server.Server{},
		Cfg:    *cfg,
	}
	cortex.Server.HTTP = mux.NewRouter()

	_, err := cortex.initAPI()
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/config", nil)
	resp := httptest.NewRecorder()
	cortex.Server.HTTP.ServeHTTP(resp, req)

	require.Equal(t, 200, resp.Code)

	body := resp.Body.String()

	// Verify the sentinel never appears in cleartext.
	assert.NotContains(t, body, sentinel, "a flagext.Secret value was leaked in /config output")

	// Verify at least one masked value is present (sanity check).
	assert.Contains(t, body, "********", "expected masked secrets in /config output")
}

// TestConfig_SensitiveFieldTypes verifies that every struct field in Config
// whose YAML tag name suggests a credential uses flagext.Secret, not string.
// This catches new password/secret fields added as plain strings even if they
// have no default value.
func TestConfig_SensitiveFieldTypes(t *testing.T) {
	sensitivePattern := regexp.MustCompile(`(?i)^(password|secret|secret_key|application_credential_secret|basic_auth_password)$`)
	secretType := reflect.TypeFor[flagext.Secret]()

	var violations []string
	checkSensitiveFields(reflect.TypeFor[Config](), "", sensitivePattern, secretType, &violations)

	for _, v := range violations {
		t.Errorf("field should use flagext.Secret, not string: %s", v)
	}
}

// checkSensitiveFields recursively walks a type and reports any string field
// whose YAML tag matches the sensitive pattern.
func checkSensitiveFields(t reflect.Type, prefix string, pattern *regexp.Regexp, secretType reflect.Type, violations *[]string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for f := range t.Fields() {
		path := prefix + f.Name

		yamlTag := f.Tag.Get("yaml")
		yamlName := strings.Split(yamlTag, ",")[0]

		if pattern.MatchString(yamlName) && f.Type.Kind() == reflect.String {
			*violations = append(*violations, path+" (yaml:\""+yamlName+"\")")
		}

		ft := f.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && ft != secretType {
			checkSensitiveFields(ft, path+".", pattern, secretType, violations)
		}
	}
}

// setAllSecrets recursively walks a reflect.Value and sets every flagext.Secret
// field's Value to the given sentinel string.
func setAllSecrets(v reflect.Value, sentinel string) {
	switch v.Kind() {
	case reflect.Ptr:
		if !v.IsNil() {
			setAllSecrets(v.Elem(), sentinel)
		}
	case reflect.Struct:
		secretType := reflect.TypeFor[flagext.Secret]()
		for _, f := range v.Fields() {
			if !f.CanSet() {
				continue
			}
			if f.Type() == secretType {
				f.Set(reflect.ValueOf(flagext.Secret{Value: sentinel}))
			} else {
				setAllSecrets(f, sentinel)
			}
		}
	}
}
