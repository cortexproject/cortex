package httpgrpcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
)

func TestGetHeader(t *testing.T) {
	tests := []struct {
		name     string
		headers  []*httpgrpc.Header
		key      string
		expected string
	}{
		{
			name:     "returns empty string for missing key",
			headers:  nil,
			key:      "X-Missing",
			expected: "",
		},
		{
			name: "returns first value for existing key",
			headers: []*httpgrpc.Header{
				{Key: "Content-Type", Values: []string{"application/json", "text/plain"}},
			},
			key:      "Content-Type",
			expected: "application/json",
		},
		{
			name: "does not match different key",
			headers: []*httpgrpc.Header{
				{Key: "Accept", Values: []string{"text/html"}},
			},
			key:      "Content-Type",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httpgrpc.HTTPRequest{Headers: tt.headers}
			result := GetHeader(req, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetHeaderValues(t *testing.T) {
	tests := []struct {
		name     string
		headers  []*httpgrpc.Header
		key      string
		expected []string
	}{
		{
			name:     "returns empty slice for missing key",
			headers:  nil,
			key:      "X-Missing",
			expected: []string{},
		},
		{
			name: "returns all values for existing key",
			headers: []*httpgrpc.Header{
				{Key: "Accept", Values: []string{"text/html", "application/json"}},
			},
			key:      "Accept",
			expected: []string{"text/html", "application/json"},
		},
		{
			name: "returns values from first matching header only",
			headers: []*httpgrpc.Header{
				{Key: "X-Custom", Values: []string{"first"}},
				{Key: "X-Custom", Values: []string{"second"}},
			},
			key:      "X-Custom",
			expected: []string{"first"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httpgrpc.HTTPRequest{Headers: tt.headers}
			result := GetHeaderValues(req, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
