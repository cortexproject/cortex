package configs

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var legacyRulesFile = `ALERT TestAlert
IF up == 0
FOR 5m
LABELS { severity = "critical" }
ANNOTATIONS {
	message = "I am a message"
}`

var ruleFile = `groups:
- name: example
  rules:
  - alert: TestAlert
    expr: up == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      message: I am a message`

func TestUnmarshalLegacyConfigWithMissingRuleFormatVersionSucceeds(t *testing.T) {
	actual := Config{}
	buf := []byte(`{"rules_files": {"a": "b"}}`)
	assert.Nil(t, json.Unmarshal(buf, &actual))

	expected := Config{
		RulesConfig: RulesConfig{
			Files: map[string]string{
				"a": "b",
			},
			FormatVersion: RuleFormatV1,
		},
	}

	assert.Equal(t, expected, actual)
}


func TestParseLegacyAlerts(t *testing.T) {
	parsed, err := promql.ParseExpr("up == 0")
	require.NoError(t, err)
	rule := rules.NewAlertingRule(
		"TestAlert",
		parsed,
		5*time.Minute,
		labels.Labels{
			labels.Label{Name: "severity", Value: "critical"},
		},
		labels.Labels{
			labels.Label{Name: "message", Value: "I am a message"},
		},
		nil,
		true,
		log.With(util.Logger, "alert", "TestAlert"),
	)

	for i, tc := range []struct {
		cfg      RulesConfig
		expected map[string][]rules.Rule
	}{
		{
			cfg: RulesConfig{
				FormatVersion: RuleFormatV1,
				Files: map[string]string{
					"legacy.rules": `
		ALERT TestAlert
		IF up == 0
		FOR 5m
		LABELS { severity = "critical" }
		ANNOTATIONS {
			message = "I am a message"
		}
		`,
				},
			},
			expected: map[string][]rules.Rule{
				"legacy.rules": {rule},
			},
		},
		{
			cfg: RulesConfig{
				FormatVersion: RuleFormatV2,
				Files: map[string]string{
					"alerts.yaml": `
groups:
- name: example
  rules:
  - alert: TestAlert
    expr: up == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      message: I am a message
`,
				},
			},
			expected: map[string][]rules.Rule{
				"example;alerts.yaml": {rule},
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			rules, err := tc.cfg.Parse()
			require.NoError(t, err)
			require.Equal(t, tc.expected, rules)
		})
	}
}


func TestParseFormatted(t *testing.T) {
	dur, err := model.ParseDuration("5m")
	require.NoError(t, err)

	rules := []rulefmt.Rule{
		rulefmt.Rule{
			Alert: "TestAlert",
			Expr:  "up == 0",
			For:   dur,
			Labels: map[string]string{
				"severity": "critical",
			},
			Annotations: map[string]string{
				"message": "I am a message",
			},
		},
	}

	for i, tc := range []struct {
		cfg      RulesConfig
		expected map[string]rulefmt.RuleGroups
	}{
		{
			cfg: RulesConfig{
				FormatVersion: RuleFormatV1,
				Files: map[string]string{
					"legacy.rules": legacyRulesFile,
				},
			},
			expected: map[string]rulefmt.RuleGroups{
				"legacy.rules": rulefmt.RuleGroups{
					Groups: []rulefmt.RuleGroup{
						rulefmt.RuleGroup{
							Name:  "rg:legacy.rules",
							Rules: rules,
						},
					},
				},
			},
		},
		{
			cfg: RulesConfig{
				FormatVersion: RuleFormatV2,
				Files: map[string]string{
					"alerts.yaml": ruleFile,
				},
			},
			expected: map[string]rulefmt.RuleGroups{
				"alerts.yaml": rulefmt.RuleGroups{
					Groups: []rulefmt.RuleGroup{
						rulefmt.RuleGroup{
							Name:  "example",
							Rules: rules,
						},
					},
				},
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			rules, err := tc.cfg.ParseFormatted()
			require.NoError(t, err)
			require.Equal(t, tc.expected, rules)
		})
	}
}
