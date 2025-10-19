package util

import "gopkg.in/yaml.v2"

// YAMLMarshalUnmarshal utility function that converts a YAML interface in a map
// doing marshal and unmarshal of the parameter
func YAMLMarshalUnmarshal(in any) (map[any]any, error) {
	yamlBytes, err := yaml.Marshal(in)
	if err != nil {
		return nil, err
	}

	object := make(map[any]any)
	if err := yaml.Unmarshal(yamlBytes, object); err != nil {
		return nil, err
	}

	return object, nil
}
