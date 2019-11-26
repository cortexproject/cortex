package runtimeconfig

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
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

func testLoadOverrides(filename string) (interface{}, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var overrides = &testOverrides{}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	if err := decoder.Decode(&overrides); err != nil {
		return nil, err
	}

	return overrides, nil
}

func TestNewOverridesManager(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "test-validation")
	require.NoError(t, err)

	defer func() {
		// Clean up
		require.NoError(t, tempFile.Close())
		require.NoError(t, os.Remove(tempFile.Name()))
	}()

	_, err = tempFile.WriteString(`overrides:
  user1:
    limit2: 150`)
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing NewRuntimeConfigManager with overrides reload config set
	overridesManagerConfig := ManagerConfig{
		ReloadPeriod: time.Second,
		LoadPath:     tempFile.Name(),
		Loader:       testLoadOverrides,
	}

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig)
	require.NoError(t, err)

	// Cleaning up
	overridesManager.Stop()

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
}

func TestOverridesManager_Listener(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "test-validation")
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	defer func() {
		// Clean up
		require.NoError(t, os.Remove(tempFile.Name()))
	}()

	err = ioutil.WriteFile(tempFile.Name(), []byte(`overrides:
  user1:
    limit2: 150`), 0600)
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing NewRuntimeConfigManager with overrides reload config set
	overridesManagerConfig := ManagerConfig{
		ReloadPeriod: time.Second,
		LoadPath:     tempFile.Name(),
		Loader:       testLoadOverrides,
	}

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig)
	require.NoError(t, err)

	// listeners are called asynchronously
	ch := make(chan interface{})
	overridesManager.AddListener(func(newConfig interface{}) {
		ch <- newConfig
	})

	// rewrite file
	err = ioutil.WriteFile(tempFile.Name(), []byte(`overrides:
  user2:
    limit2: 200`), 0600)
	require.NoError(t, err)

	// reload
	err = overridesManager.loadConfig()
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

	// Cleaning up
	overridesManager.Stop()

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
}
