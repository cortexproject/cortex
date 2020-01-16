package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_findFlagsPrefix(t *testing.T) {
	tests := []struct {
		input    []string
		expected []string
	}{
		{
			input:    []string{},
			expected: []string{},
		},
		{
			input:    []string{""},
			expected: []string{""},
		},
		{
			input:    []string{"", ""},
			expected: []string{"", ""},
		},
		{
			input:    []string{"foo", "foo", "foo"},
			expected: []string{"", "", ""},
		},
		{
			input:    []string{"ruler.endpoint", "alertmanager.endpoint"},
			expected: []string{"ruler", "alertmanager"},
		},
		{
			input:    []string{"ruler.endpoint.address", "alertmanager.endpoint.address"},
			expected: []string{"ruler", "alertmanager"},
		},
		{
			input:    []string{"ruler.first.address", "ruler.second.address"},
			expected: []string{"ruler.first", "ruler.second"},
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, findFlagsPrefix(test.input))
	}
}
