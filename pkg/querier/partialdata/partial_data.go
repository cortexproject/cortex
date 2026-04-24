package partialdata

import (
	"errors"
)

type IsCfgEnabledFunc func(userID string) bool

var ErrPartialData = errors.New("query result may contain partial data")

func IsPartialDataError(err error) bool {
	return errors.Is(err, ErrPartialData)
}
