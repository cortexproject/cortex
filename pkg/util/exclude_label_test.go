package util

import (
	"testing"
)

func Test_ParseFile(t *testing.T) {

	// tests := []struct {
	// 	name    string
	// 	wantOut Conf
	// 	input   []byte
	// 	err     error
	// }{
	// 	{
	// 		name: "test1",
	// 		input: []byte(`
	//                       exclude_labels:
	//                         12768:
	//                         - metric_name: go_gc_duration_seconds
	//                           label_name: instance
	//                     `),
	// 		wantOut: Conf{
	// 			ExcludeLabels: map[string]metrics{
	// 				"12768": []Metric{{
	// 					MetricName: "go_gc_duration_seconds",
	// 					LabelName:  "instance",
	// 				},
	// 				},
	// 			},
	// 		},
	// 	},
	// 	{
	// 		name: "test2",
	// 		input: []byte(`
	//                       exclude_labels:
	//                         12768:
	//                         - metric_name: go_gc_duration_seconds
	//                     `),
	// 		err: errLabelNameMissing,
	// 	},
	// 	{
	// 		name: "test3",
	// 		input: []byte(`
	//                       exclude_labels:
	//                         12768:
	//                         - label_name: instance

	//                     `),
	// 		err: errMetricNameMissing,
	// 	},
	// }
	// for _, testData := range tests {
	// 	testData := testData

	// 	t.Run(testData.name, func(t *testing.T) {
	// 		actualConfig, err := ParseFile(testData.input)
	// 		assert.NoError(t, err)
	// 		validationError := actualConfig.validate()
	// 		assert.Equal(t, testData.err, validationError)
	// 		if err == nil && validationError == nil {
	// 			require.Equal(t, testData.wantOut, actualConfig)
	// 		}
	// 	})
	// }

}

// func ParseFile(yamlFile []byte) (Conf, error) {
// 	var c Conf
// 	err := yaml.Unmarshal(yamlFile, &c)
// 	return c, err
// }
