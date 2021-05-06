package validation

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/util"
)

// When changing this, please update documentation for Limits.NotificationIntegrationLimits field.
var allowedIntegrationNames = []string{
	"webhook", "email", "pagerduty", "opsgenie", "wechat", "slack", "victorops", "pushover",
}

type NotificationLimitsMap map[string]NotificationLimits

// String implements flag.Value
func (m NotificationLimitsMap) String() string {
	out, err := json.Marshal(map[string]NotificationLimits(m))
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

// Set implements flag.Value
func (m NotificationLimitsMap) Set(s string) error {
	newMap := map[string]NotificationLimits{}
	return m.updateMap(json.Unmarshal([]byte(s), &newMap), newMap)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (m NotificationLimitsMap) UnmarshalYAML(unmarshal func(interface{}) error) error {
	newMap := map[string]NotificationLimits{}
	return m.updateMap(unmarshal(newMap), newMap)
}

func (m NotificationLimitsMap) updateMap(err error, newMap map[string]NotificationLimits) error {
	if err != nil {
		return err
	}

	for k, v := range newMap {
		if !util.StringsContain(allowedIntegrationNames, k) {
			return errors.Errorf("unknown integration name: %s", k)
		}
		m[k] = v
	}
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (m NotificationLimitsMap) MarshalYAML() (interface{}, error) {
	return map[string]NotificationLimits(m), nil
}
