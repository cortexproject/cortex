package configinit

import "github.com/prometheus/common/model"

func init() {
	// nolint:staticcheck
	model.NameValidationScheme = model.LegacyValidation
}
