package runtimeconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type TestLimits struct {
	Limit1 int `json:"limit1"`
	Limit2 int `json:"limit2"`
}

// WARNING: THIS GLOBAL VARIABLE COULD LEAD TO UNEXPECTED BEHAVIOUR WHEN RUNNING MULTIPLE DIFFERENT TESTS
var defaultTestLimits *TestLimits

type testOverrides struct {
	Overrides map[string]*TestLimits `yaml:"overrides"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *TestLimits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultTestLimits != nil {
		*l = *defaultTestLimits
	}
	type plain TestLimits
	return unmarshal((*plain)(l))
}

func testLoadOverrides(r io.Reader) (interface{}, error) {
	var overrides = &testOverrides{}

	decoder := yaml.NewDecoder(r)
	decoder.SetStrict(true)
	if err := decoder.Decode(&overrides); err != nil {
		return nil, err
	}
	return overrides, nil
}

func newTestOverridesManagerConfig(t *testing.T, i int32) (*atomic.Int32, Config) {
	var config = atomic.NewInt32(i)

	// create empty file
	tempFile, err := os.CreateTemp("", "test-validation")
	require.NoError(t, err)

	t.Cleanup(func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	})

	// testing NewRuntimeConfigManager with overrides reload config set
	return config, Config{
		ReloadPeriod: 5 * time.Second,
		LoadPath:     tempFile.Name(),
		Loader: func(_ io.Reader) (i interface{}, err error) {
			val := int(config.Load())
			return val, nil
		},
		StorageConfig: bucket.Config{Backend: bucket.Filesystem},
	}
}

func TestNewOverridesManager(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-validation")
	require.NoError(t, err)

	defer func() {
		// Clean up
		require.NoError(t, tempFile.Close())
		require.NoError(t, os.Remove(tempFile.Name()))
	}()

	config := `overrides:
  user1:
    limit2: 150`
	_, err = tempFile.WriteString(config)
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing NewRuntimeConfigManager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod:  time.Second,
		LoadPath:      tempFile.Name(),
		Loader:        testLoadOverrides,
		StorageConfig: bucket.Config{Backend: bucket.Filesystem},
	}

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger(), mockBucketClientFactory([]byte(config)))
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())

	// Defaults to filesystem backend
	require.Equal(t, bucket.Filesystem, overridesManager.cfg.StorageConfig.Backend)
	require.Equal(t, "", overridesManager.cfg.StorageConfig.Filesystem.Directory)
	require.NotNil(t, overridesManager.bucketClientFactory)
}

func TestManager_ListenerWithDefaultLimits(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-validation")
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	defer func() {
		// Clean up
		require.NoError(t, os.Remove(tempFile.Name()))
	}()

	config1 := []byte(`overrides:
  user1:
    limit2: 150`)
	config2 := []byte(`overrides:
  user2:
    limit2: 200`)

	err = os.WriteFile(tempFile.Name(), config1, 0600)
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing NewRuntimeConfigManager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod:  time.Second,
		LoadPath:      tempFile.Name(),
		Loader:        testLoadOverrides,
		StorageConfig: bucket.Config{Backend: bucket.Filesystem},
	}

	reg := prometheus.NewPedanticRegistry()

	overridesManager, err := New(overridesManagerConfig, reg, log.NewNopLogger(), mockBucketClientFactory(config1, config2))
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// check if the metrics is set to the config map value before
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP runtime_config_hash Hash of the currently active runtime config file.
					# TYPE runtime_config_hash gauge
					runtime_config_hash{sha256="%s"} 1
					# HELP runtime_config_last_reload_successful Whether the last runtime-config reload attempt was successful.
					# TYPE runtime_config_last_reload_successful gauge
					runtime_config_last_reload_successful 1
				`, fmt.Sprintf("%x", sha256.Sum256(config1))))))

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(1)

	// rewrite file
	err = os.WriteFile(tempFile.Name(), config2, 0600)
	require.NoError(t, err)

	// reload
	err = overridesManager.loadConfig(context.TODO())
	require.NoError(t, err)

	var newValue interface{}
	select {
	case newValue = <-ch:
		// ok
	case <-time.After(time.Second):
		t.Fatal("listener was not called")
	}

	to := newValue.(*testOverrides)
	require.Equal(t, 200, to.Overrides["user2"].Limit2) // new overrides
	require.Equal(t, 100, to.Overrides["user2"].Limit1) // from defaults

	// check if the metrics have been updated
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP runtime_config_hash Hash of the currently active runtime config file.
					# TYPE runtime_config_hash gauge
					runtime_config_hash{sha256="%s"} 1
					# HELP runtime_config_last_reload_successful Whether the last runtime-config reload attempt was successful.
					# TYPE runtime_config_last_reload_successful gauge
					runtime_config_last_reload_successful 1
				`, fmt.Sprintf("%x", sha256.Sum256(config2))))))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
}

func TestManager_ListenerChannel(t *testing.T) {
	config, overridesManagerConfig := newTestOverridesManagerConfig(t, 555)

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger(), mockBucketClientFactory([]byte{}, []byte{}, []byte{}))
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(1)

	err = overridesManager.loadConfig(context.TODO())
	require.NoError(t, err)

	select {
	case newValue := <-ch:
		require.Equal(t, 555, newValue)
	case <-time.After(time.Second):
		t.Fatal("listener was not called")
	}

	config.Store(1111)
	err = overridesManager.loadConfig(context.TODO())
	require.NoError(t, err)

	select {
	case newValue := <-ch:
		require.Equal(t, 1111, newValue)
	case <-time.After(time.Second):
		t.Fatal("listener was not called")
	}

	overridesManager.CloseListenerChannel(ch)
	select {
	case _, ok := <-ch:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func TestManager_StopClosesListenerChannels(t *testing.T) {
	_, overridesManagerConfig := newTestOverridesManagerConfig(t, 555)

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger(), mockBucketClientFactory([]byte{}))
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(0)

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	select {
	case _, ok := <-ch:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func TestManager_ShouldFastFailOnInvalidConfigAtStartup(t *testing.T) {
	// Create an invalid runtime config file.
	tempFile, err := os.CreateTemp("", "invalid-config")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.Remove(tempFile.Name()))
	})

	config := []byte("!invalid!")
	_, err = tempFile.Write(config)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	// Create the config manager and start it.
	cfg := Config{
		ReloadPeriod:  time.Second,
		LoadPath:      tempFile.Name(),
		Loader:        testLoadOverrides,
		StorageConfig: bucket.Config{Backend: bucket.Filesystem},
	}

	m, err := New(cfg, nil, log.NewNopLogger(), mockBucketClientFactory(config))
	require.NoError(t, err)
	require.Error(t, services.StartAndAwaitRunning(context.Background(), m))
}

func TestManger_ShouldFastFailOnMissingConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		errorMessage string
	}{
		{
			name:         "empty load path",
			cfg:          Config{},
			errorMessage: "LoadPath is empty",
		},
		{
			name: "empty storage backend",
			cfg: Config{
				LoadPath: "fileLoadPath",
			},
			errorMessage: "Backend should not be explicitly empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, nil, log.NewNopLogger(), mockBucketClientFactory([]byte{}))
			require.ErrorContains(t, err, tt.errorMessage)
		})
	}
}

func TestManager_GetsRuntimeConfigFromBackendStore(t *testing.T) {
	fileName := "runtime-config"
	config := []byte(`overrides:
  user2:
    limit2: 200`)
	bucketClient := createMockBucketClient(config)
	manager := Manager{
		cfg: Config{
			LoadPath:      fileName,
			Loader:        testLoadOverrides,
			StorageConfig: bucket.Config{Backend: bucket.Filesystem},
		},
		configLoadSuccess: promauto.NewGauge(prometheus.GaugeOpts{Name: "mockLoadSuccess"}),
		configHash: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mockHash",
		}, []string{"sha256"}),
		bucketClient: bucketClient,
	}

	err := manager.loadConfig(context.TODO())

	require.NoError(t, err)
	bucketClient.AssertExpectations(t)
}

func mockBucketClientFactory(configs ...[]byte) BucketClientFactory {
	return func(ctx context.Context) (objstore.Bucket, error) {
		return createMockBucketClient(configs...), nil
	}
}

func createMockBucketClient(configs ...[]byte) *bucket.ClientMock {
	bucketClient := bucket.ClientMock{}
	for _, config := range configs {
		bucketClient.On("Get", mock.Anything, mock.Anything).Return(io.NopCloser(bytes.NewBuffer(config)), nil).Once()
	}
	return &bucketClient
}
