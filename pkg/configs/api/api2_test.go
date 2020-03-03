package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseConfigFormat(t *testing.T) {
	tests := []struct {
		name          string
		defaultFormat string
		expected      string
	}{
		{"", FormatInvalid, FormatInvalid},
		{"", FormatJSON, FormatJSON},
		{"application/json", FormatInvalid, FormatJSON},
		{"application/yaml", FormatInvalid, FormatYAML},
		{"application/json, application/yaml", FormatInvalid, FormatJSON},
		{"application/yaml, application/json", FormatInvalid, FormatYAML},
		{"text/plain, application/yaml", FormatInvalid, FormatYAML},
		{"application/yaml; a=1", FormatInvalid, FormatYAML},
	}
	for _, test := range tests {
		t.Run(test.name+"_"+test.expected, func(t *testing.T) {
			actual := parseConfigFormat(test.name, test.defaultFormat)
			assert.Equal(t, test.expected, actual)
		})
	}
}
