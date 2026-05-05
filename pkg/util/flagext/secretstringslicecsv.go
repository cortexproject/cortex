package flagext

import "strings"

// SecretStringSliceCSV is a slice of strings that is parsed from a comma-separated string.
// It implements flag.Value and yaml Marshalers, but masks the value when marshaled to YAML
// so that secrets are not exposed via the /config endpoint.
type SecretStringSliceCSV struct {
	values []string
}

// String implements flag.Value
func (v SecretStringSliceCSV) String() string {
	return strings.Join(v.values, ",")
}

// Set implements flag.Value
func (v *SecretStringSliceCSV) Set(s string) error {
	if s == "" {
		v.values = nil
		return nil
	}
	v.values = strings.Split(s, ",")
	return nil
}

// Value returns the underlying string slice.
func (v SecretStringSliceCSV) Value() []string {
	return v.values
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *SecretStringSliceCSV) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	return v.Set(s)
}

// MarshalYAML implements yaml.Marshaler.
// The value is masked to avoid exposing secrets via the /config endpoint.
func (v SecretStringSliceCSV) MarshalYAML() (any, error) {
	if len(v.values) == 0 {
		return "", nil
	}
	return "********", nil
}
