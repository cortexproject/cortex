package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestExcludeLabels_Validate(t *testing.T) {

	tests := []struct {
		name    string
		wantOut ExcludeLabels
		input   []byte
		err     error
	}{
		{
			name: "test1",
			input: []byte(`
                "12768":
                  - metric_name: go_gc_duration_seconds
                    label_name: instance
                `),
			wantOut: ExcludeLabels{
				"12768": []Metric{{
					MetricName: "go_gc_duration_seconds",
					LabelName:  "instance",
				},
				},
			},
		},
		{
			name: "test2",
			input: []byte(`
                12768:
                  - metric_name: go_gc_duration_seconds
                `),
			err: errLabelNameMissing,
		},
		{
			name: "test3",
			input: []byte(`
                12768:
                  - label_name: instance
            `),
			err: errMetricNameMissing,
		},
	}
	for _, testData := range tests {
		testData := testData

		t.Run(testData.name, func(t *testing.T) {
			actualConfig, err := ParseFile(testData.input)
			assert.NoError(t, err)
			validationError := actualConfig.Validate()
			assert.Equal(t, testData.err, validationError)
			if err == nil && validationError == nil {
				require.Equal(t, testData.wantOut, actualConfig)
			}
		})
	}

}

func ParseFile(yamlFile []byte) (ExcludeLabels, error) {
	var excfg ExcludeLabels
	err := yaml.Unmarshal(yamlFile, &excfg)
	return excfg, err
}
