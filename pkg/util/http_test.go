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
		name                string
		headers             map[string]string
		tmpl                string
		expectedOutput      string
		expectedContentType string
		value               testStruct
	}{
		{
			name: "Test Renders json",
			headers: map[string]string{
				"Accept": "application/json",
			},
			tmpl:                "<html></html>",
			expectedOutput:      `{"name":"testName","value":42}`,
			expectedContentType: "application/json",
			value: testStruct{
				Name:  "testName",
				Value: 42,
			},
		},
		{
			name:                "Test Renders html",
			headers:             map[string]string{},
			tmpl:                "<html>{{ .Name }}</html>",
			expectedOutput:      "<html>testName</html>",
			expectedContentType: "text/html; charset=utf-8",
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

			assert.Equal(t, tt.expectedContentType, writer.Header().Get("Content-Type"))
			assert.Equal(t, 200, writer.Code)
			assert.Equal(t, tt.expectedOutput, writer.Body.String())
		})
	}
}

func TestWriteTextResponse(t *testing.T) {
	w := httptest.NewRecorder()

	WriteTextResponse(w, "hello world")

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "hello world", w.Body.String())
	assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
}
