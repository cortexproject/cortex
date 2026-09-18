package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSecretStringSliceCSV(t *testing.T) {
	type TestStruct struct {
		Keys SecretStringSliceCSV `yaml:"keys"`
	}

	t.Run("values are masked when marshaled to YAML", func(t *testing.T) {
		var s TestStruct
		require.NoError(t, s.Keys.Set("key1,key2,key3"))

		assert.Equal(t, []string{"key1", "key2", "key3"}, s.Keys.Value())
		assert.Equal(t, "key1,key2,key3", s.Keys.String())

		actual, err := yaml.Marshal(s)
		require.NoError(t, err)
		assert.Equal(t, "keys: '********'\n", string(actual))
	})

	t.Run("values are unmarshaled correctly from YAML", func(t *testing.T) {
		var s TestStruct
		require.NoError(t, yaml.Unmarshal([]byte("keys: key1,key2,key3\n"), &s))
		assert.Equal(t, []string{"key1", "key2", "key3"}, s.Keys.Value())
	})

	t.Run("empty value marshals to empty string", func(t *testing.T) {
		var s TestStruct
		actual, err := yaml.Marshal(s)
		require.NoError(t, err)
		assert.Equal(t, "keys: \"\"\n", string(actual))
	})

	t.Run("empty string set clears values", func(t *testing.T) {
		var s TestStruct
		require.NoError(t, s.Keys.Set("key1,key2"))
		require.NoError(t, s.Keys.Set(""))
		assert.Equal(t, []string(nil), s.Keys.Value())
	})

	t.Run("trailing comma is rejected", func(t *testing.T) {
		var s TestStruct
		err := s.Keys.Set("newkey,")
		require.Error(t, err, "trailing comma must produce an error")
		assert.Nil(t, s.Keys.Value(), "values must not be updated on error")
	})

	t.Run("leading comma is rejected", func(t *testing.T) {
		var s TestStruct
		require.Error(t, s.Keys.Set(",newkey"))
	})

	t.Run("double comma is rejected", func(t *testing.T) {
		var s TestStruct
		require.Error(t, s.Keys.Set("newkey,,oldkey"))
	})

	t.Run("whitespace-only entry is rejected", func(t *testing.T) {
		var s TestStruct
		require.Error(t, s.Keys.Set("newkey, ,oldkey"))
	})

	t.Run("surrounding whitespace is trimmed from valid entries", func(t *testing.T) {
		var s TestStruct
		require.NoError(t, s.Keys.Set(" key1 , key2 "))
		assert.Equal(t, []string{"key1", "key2"}, s.Keys.Value())
	})
}
