package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDayValueYAML(t *testing.T) {
	// Test embedding of DayValue.
	{
		type TestStruct struct {
			Day DayValue `yaml:"day"`
		}

		var testStruct TestStruct
		testStruct.Day.Set("1985-06-02")
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test pointers of DayValue.
	{
		type TestStruct struct {
			Day *DayValue `yaml:"day"`
		}

		var testStruct TestStruct
		testStruct.Day = &DayValue{}
		testStruct.Day.Set("1985-06-02")
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}
}
