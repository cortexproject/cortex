package alertspb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToProto(t *testing.T) {
	tests := []struct {
		name      string
		cfg       string
		templates map[string]string
		user      string
	}{
		{
			name:      "empty config and no templates",
			cfg:       "",
			templates: nil,
			user:      "user-1",
		},
		{
			name: "config with templates",
			cfg:  "route:\n  receiver: default",
			templates: map[string]string{
				"slack.tmpl": "{{ define \"slack\" }}alert{{ end }}",
			},
			user: "user-2",
		},
		{
			name:      "empty user",
			cfg:       "global: {}",
			templates: nil,
			user:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToProto(tt.cfg, tt.templates, tt.user)

			assert.Equal(t, tt.user, result.User)
			assert.Equal(t, tt.cfg, result.RawConfig)

			if tt.templates == nil {
				assert.Empty(t, result.Templates)
			} else {
				assert.Len(t, result.Templates, len(tt.templates))
				for _, tmpl := range result.Templates {
					expectedBody, ok := tt.templates[tmpl.Filename]
					assert.True(t, ok, "unexpected template filename: %s", tmpl.Filename)
					assert.Equal(t, expectedBody, tmpl.Body)
				}
			}
		})
	}
}

func TestParseTemplates(t *testing.T) {
	tests := []struct {
		name     string
		cfg      AlertConfigDesc
		expected map[string]string
	}{
		{
			name:     "no templates",
			cfg:      AlertConfigDesc{Templates: nil},
			expected: map[string]string{},
		},
		{
			name: "single template",
			cfg: AlertConfigDesc{
				Templates: []*TemplateDesc{
					{Filename: "slack.tmpl", Body: "{{ define \"slack\" }}msg{{ end }}"},
				},
			},
			expected: map[string]string{
				"slack.tmpl": "{{ define \"slack\" }}msg{{ end }}",
			},
		},
		{
			name: "multiple templates",
			cfg: AlertConfigDesc{
				Templates: []*TemplateDesc{
					{Filename: "a.tmpl", Body: "body-a"},
					{Filename: "b.tmpl", Body: "body-b"},
				},
			},
			expected: map[string]string{
				"a.tmpl": "body-a",
				"b.tmpl": "body-b",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseTemplates(tt.cfg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToProto_ParseTemplates_roundtrip(t *testing.T) {
	templates := map[string]string{
		"email.tmpl": "{{ define \"email\" }}hello{{ end }}",
		"slack.tmpl": "{{ define \"slack\" }}alert{{ end }}",
	}
	cfg := "route:\n  receiver: default"
	user := "test-user"

	proto := ToProto(cfg, templates, user)
	result := ParseTemplates(proto)

	assert.Equal(t, templates, result)
	assert.Equal(t, cfg, proto.RawConfig)
	assert.Equal(t, user, proto.User)
}
