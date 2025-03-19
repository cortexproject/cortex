package configinit

import "github.com/prometheus/common/model"

func init() {
	model.NameValidationScheme = model.LegacyValidation
}
