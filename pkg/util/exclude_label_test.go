package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestExcludeLabels_SkipLabel(t *testing.T) {
	type inputStruct struct {
		cfg        []byte
		lbName     string
		userID     string
		metricName string
	}
	tests := []struct {
		name    string
		wantOut bool
		input   inputStruct
		err     error
	}{
		{
			name: "test1",
			input: inputStruct{
				cfg: []byte(`
                "12768":
                  - metric_name: go_gc_duration_seconds
                    label_name: instance`),
				lbName:     "instance",
				metricName: "go_gc_duration_seconds",
				userID:     "12768",
			},
			wantOut: true,
		},
		{
			name: "test2",
			input: inputStruct{
				cfg: []byte(`
                "12768":
                  - metric_name: go_gc_duration_seconds
                    label_name: somelabel`),
				lbName:     "instance",
				metricName: "go_gc_duration_seconds",
				userID:     "12768",
			},
			wantOut: false,
		},
		{
			name: "test3",
			input: inputStruct{
				cfg: []byte(`
                "12768":
                  - metric_name: somemetric
                    label_name: instance`),
				lbName:     "instance",
				metricName: "go_gc_duration_seconds",
				userID:     "12768",
			},
			wantOut: false,
		},
		{
			name: "test4",
			input: inputStruct{
				cfg: []byte(`
                "someuser":
                  - metric_name: go_gc_duration_seconds
                    label_name: instance`),
				lbName:     "instance",
				metricName: "go_gc_duration_seconds",
				userID:     "12768",
			},
			wantOut: false,
		},
	}
	for _, testData := range tests {

		t.Run(testData.name, func(t *testing.T) {
			inputData := testData.input
			actualConfig, err := ParseFile(testData.input.cfg)
			assert.NoError(t, err)
			assert.Equal(t, testData.wantOut, actualConfig.Skiplabel(inputData.lbName, inputData.metricName, inputData.userID))
		})
	}
}
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
