package runtimeconfig

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"

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

func newTestOverridesManagerConfig(t *testing.T, i int32) (*atomic.Int32, ManagerConfig) {
	config, tempFile, loader := newConfig(t, i)

	// testing NewRuntimeConfigManager with overrides reload config set
	return config, ManagerConfig{
		ReloadPeriod: 5 * time.Second,
		LoadPath:     tempFile,
		Loader:       loader,
	}
}

func newTestExtensionConfig(t *testing.T, configName string, i int32) (*atomic.Int32, ExtensionConfig) {
	config, tempFile, loader := newConfig(t, i)

	return config, ExtensionConfig{
		Name:     configName,
		LoadPath: tempFile,
		Loader:   loader,
	}
}

func newConfig(t *testing.T, i int32) (*atomic.Int32, string, Loader) {
	var config = atomic.NewInt32(i)

	// create empty file
	tempFile, err := ioutil.TempFile("", "test-validation")
	require.NoError(t, err)

	t.Cleanup(func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	})

	return config, tempFile.Name(), func(_ io.Reader) (i interface{}, err error) {
		val := int(config.Load())
		return val, nil
	}
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

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
}

func TestOverridesManager_ListenerWithDefaultLimits(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "test-validation")
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	defer func() {
		// Clean up
		require.NoError(t, os.Remove(tempFile.Name()))
	}()

	config := []byte(`overrides:
  user1:
    limit2: 150`)
	err = ioutil.WriteFile(tempFile.Name(), config, 0600)
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing NewRuntimeConfigManager with overrides reload config set
	overridesManagerConfig := ManagerConfig{
		ReloadPeriod: time.Second,
		LoadPath:     tempFile.Name(),
		Loader:       testLoadOverrides,
	}

	reg := prometheus.NewPedanticRegistry()

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig, reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// check if the metrics is set to the config map value before
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_runtime_config_hash Hash of the currently active runtime config file.
					# TYPE cortex_runtime_config_hash gauge
					cortex_runtime_config_hash{sha256="%s"} 1
					# HELP cortex_runtime_config_last_reload_successful Whether the last runtime-config reload attempt was successful.
					# TYPE cortex_runtime_config_last_reload_successful gauge
					cortex_runtime_config_last_reload_successful 1
				`, fmt.Sprintf("%x", sha256.Sum256(config))))))

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(1)

	// rewrite file
	config = []byte(`overrides:
  user2:
    limit2: 200`)
	err = ioutil.WriteFile(tempFile.Name(), config, 0600)
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

	// check if the metrics have been updated
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_runtime_config_hash Hash of the currently active runtime config file.
					# TYPE cortex_runtime_config_hash gauge
					cortex_runtime_config_hash{sha256="%s"} 1
					# HELP cortex_runtime_config_last_reload_successful Whether the last runtime-config reload attempt was successful.
					# TYPE cortex_runtime_config_last_reload_successful gauge
					cortex_runtime_config_last_reload_successful 1
				`, fmt.Sprintf("%x", sha256.Sum256(config))))))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
}

func TestOverridesManager_ListenerChannel(t *testing.T) {
	config, overridesManagerConfig := newTestOverridesManagerConfig(t, 555)

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(1)

	err = overridesManager.loadConfig()
	require.NoError(t, err)

	select {
	case newValue := <-ch:
		require.Equal(t, 555, newValue)
	case <-time.After(time.Second):
		t.Fatal("listener was not called")
	}

	config.Store(1111)
	err = overridesManager.loadConfig()
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

func TestOverridesManager_StopClosesListenerChannels(t *testing.T) {
	_, overridesManagerConfig := newTestOverridesManagerConfig(t, 555)

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig, nil)
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

func TestOverridesManager_LoadExtensionConfig(t *testing.T) {
	myExtensionName1 := "testExtension1"
	myExtensionName2 := "testExtension2"

	_, overridesManagerConfig := newTestOverridesManagerConfig(t, 111)
	_, extensionConfig1 := newTestExtensionConfig(t, myExtensionName1, 3)
	_, extensionConfig2 := newTestExtensionConfig(t, myExtensionName2, 42)

	overridesManager, err := NewRuntimeConfigManager(overridesManagerConfig, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	_ = overridesManager.AddExtension(extensionConfig1)
	_ = overridesManager.AddExtension(extensionConfig2)

	mainCh := overridesManager.CreateListenerChannel(1)
	extensionCh1 := overridesManager.CreateListenerChannel(1, myExtensionName1)
	extensionCh2 := overridesManager.CreateListenerChannel(1, myExtensionName2)
	mixedCh1 := overridesManager.CreateListenerChannel(2, DefaultConfigName, myExtensionName1)
	mixedCh2 := overridesManager.CreateListenerChannel(2, myExtensionName1, myExtensionName2)

	err = overridesManager.loadConfig()
	require.NoError(t, err)

	expectingMsgsOnMainCh := map[int]struct{}{
		111: {},
	}
	expectingMsgsOnExtensionCh1 := map[int]struct{}{
		3: {},
	}
	expectingMsgsOnExtensionCh2 := map[int]struct{}{
		42: {},
	}
	expectingMsgsOnMixedCh1 := map[int]struct{}{
		111: {},
		3:   {},
	}
	expectingMsgsOnMixedCh2 := map[int]struct{}{
		3:  {},
		42: {},
	}

	timeoutCh := time.After(time.Second)
	for len(expectingMsgsOnMainCh)+len(expectingMsgsOnExtensionCh1)+len(expectingMsgsOnExtensionCh2)+len(expectingMsgsOnMixedCh1)+len(expectingMsgsOnMixedCh2) > 0 {
		select {
		case msgUntyped := <-mainCh:
			msg := msgUntyped.(int)
			if _, ok := expectingMsgsOnMainCh[msg]; ok {
				delete(expectingMsgsOnMainCh, msg)
			} else {
				t.Fatalf("Received unexpected msg %d on main chan", msg)
			}
		case msgUntyped := <-extensionCh1:
			msg := msgUntyped.(int)
			if _, ok := expectingMsgsOnExtensionCh1[msg]; ok {
				delete(expectingMsgsOnExtensionCh1, msg)
			} else {
				t.Fatalf("Received unexpected msg %d on main chan", msg)
			}
		case msgUntyped := <-extensionCh2:
			msg := msgUntyped.(int)
			if _, ok := expectingMsgsOnExtensionCh2[msg]; ok {
				delete(expectingMsgsOnExtensionCh2, msg)
			} else {
				t.Fatalf("Received unexpected msg %d on main chan", msg)
			}
		case msgUntyped := <-mixedCh1:
			msg := msgUntyped.(int)
			if _, ok := expectingMsgsOnMixedCh1[msg]; ok {
				delete(expectingMsgsOnMixedCh1, msg)
			} else {
				t.Fatalf("Received unexpected msg %d on main chan", msg)
			}
		case msgUntyped := <-mixedCh2:
			msg := msgUntyped.(int)
			if _, ok := expectingMsgsOnMixedCh2[msg]; ok {
				delete(expectingMsgsOnMixedCh2, msg)
			} else {
				t.Fatalf("Received unexpected msg %d on main chan", msg)
			}
		case <-timeoutCh:
			t.Fatalf("Timed out waiting for messages")
		}
	}
}
