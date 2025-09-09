package util

import (
	"fmt"
	"reflect"
)

// DiffConfig utility function that returns the diff between two config map objects
func DiffConfig(defaultConfig, actualConfig map[any]any) (map[any]any, error) {
	output := make(map[any]any)

	for key, value := range actualConfig {

		defaultValue, ok := defaultConfig[key]
		if !ok {
			output[key] = value
			continue
		}

		switch v := value.(type) {
		case int:
			defaultV, ok := defaultValue.(int)
			if !ok || defaultV != v {
				output[key] = v
			}
		case string:
			defaultV, ok := defaultValue.(string)
			if !ok || defaultV != v {
				output[key] = v
			}
		case bool:
			defaultV, ok := defaultValue.(bool)
			if !ok || defaultV != v {
				output[key] = v
			}
		case []any:
			defaultV, ok := defaultValue.([]any)
			if !ok || !reflect.DeepEqual(defaultV, v) {
				output[key] = v
			}
		case float64:
			defaultV, ok := defaultValue.(float64)
			if !ok || !reflect.DeepEqual(defaultV, v) {
				output[key] = v
			}
		case nil:
			if defaultValue != nil {
				output[key] = v
			}
		case map[any]any:
			defaultV, ok := defaultValue.(map[any]any)
			if !ok {
				output[key] = value
			}
			diff, err := DiffConfig(defaultV, v)
			if err != nil {
				return nil, err
			}
			if len(diff) > 0 {
				output[key] = diff
			}
		default:
			return nil, fmt.Errorf("unsupported type %T", v)
		}
	}

	return output, nil
}
