package validation

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

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *TestLimits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultTestLimits != nil {
		*l = *defaultTestLimits
	}
	type plain TestLimits
	return unmarshal((*plain)(l))
}

func testLoadOverrides(filename string) (map[string]interface{}, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var overrides struct {
		Overrides map[string]*TestLimits `yaml:"overrides"`
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	if err := decoder.Decode(&overrides); err != nil {
		return nil, err
	}

	overridesAsInterface := map[string]interface{}{}
	for k := range overrides.Overrides {
		overridesAsInterface[k] = overrides.Overrides[k]
	}

	return overridesAsInterface, nil
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

	// testing NewOverridesManager with overrides reload config set
	overridesManagerConfig := OverridesManagerConfig{
		OverridesReloadPeriod: time.Second,
		OverridesLoadPath:     tempFile.Name(),
		OverridesLoader:       testLoadOverrides,
		Defaults:              defaultTestLimits,
	}

	var overridesManager *OverridesManager
	done := make(chan struct{})

	go func() {
		overridesManager, err = NewOverridesManager(overridesManagerConfig)
		close(done)
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("failed to get a response from NewOverridesManager() before timeout")
	case <-done:
	}
	require.NoError(t, err)

	// Cleaning up
	overridesManager.Stop()
}

func TestOverridesManager_GetLimits(t *testing.T) {
	defaultTestLimits = &TestLimits{Limit1: 100}
	overridesManagerConfig := OverridesManagerConfig{
		OverridesReloadPeriod: 0,
		OverridesLoadPath:     "",
		OverridesLoader:       testLoadOverrides,
		Defaults:              defaultTestLimits,
	}

	overridesManager, err := NewOverridesManager(overridesManagerConfig)
	require.NoError(t, err)

	require.Equal(t, 100, overridesManager.GetLimits("user1").(*TestLimits).Limit1)
	require.Equal(t, 0, overridesManager.GetLimits("user1").(*TestLimits).Limit2)

	// Setting up perTenantOverrides for user user1
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

	overridesManager.cfg.OverridesLoadPath = tempFile.Name()
	require.NoError(t, overridesManager.loadOverrides())

	// Checking whether overrides were enforced
	require.Equal(t, 100, overridesManager.GetLimits("user1").(*TestLimits).Limit1)
	require.Equal(t, 150, overridesManager.GetLimits("user1").(*TestLimits).Limit2)

	// Verifying user2 limits are not impacted by overrides
	require.Equal(t, 100, overridesManager.GetLimits("user2").(*TestLimits).Limit1)
	require.Equal(t, 0, overridesManager.GetLimits("user2").(*TestLimits).Limit2)

	// Cleaning up
	overridesManager.Stop()
}
