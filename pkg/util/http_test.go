package util

import (
	"html/template"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderHTTPResponse(t *testing.T) {
	type testStruct struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	tests := []struct {
		name     string
		headers  map[string]string
		tmpl     string
		expected string
		value    testStruct
	}{
		{
			name: "Test Renders json",
			headers: map[string]string{
				"Accept": "application/json",
			},
			tmpl:     "<html></html>",
			expected: `{"name":"testName","value":42}`,
			value: testStruct{
				Name:  "testName",
				Value: 42,
			},
		},
		{
			name:     "Test Renders html",
			headers:  map[string]string{},
			tmpl:     "<html>{{ .Name }}</html>",
			expected: "<html>testName</html>",
			value: testStruct{
				Name:  "testName",
				Value: 42,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := template.Must(template.New("webpage").Parse(tt.tmpl))
			writer := httptest.NewRecorder()
			request := httptest.NewRequest("GET", "/", nil)

			for k, v := range tt.headers {
				request.Header.Add(k, v)
			}

			RenderHTTPResponse(writer, tt.value, tmpl, request)

			assert.Equal(t, tt.expected, writer.Body.String())
		})
	}
}
