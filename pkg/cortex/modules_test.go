package cortex

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gorilla/mux"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/cortexpb"
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
			MonitoredResources: []string{"invalid"},
		},
	}

	// log warning message and spin up other cortex services
	_, err := cortex.initResourceMonitor()
	require.ErrorContains(t, err, "unknown resource type")
}
