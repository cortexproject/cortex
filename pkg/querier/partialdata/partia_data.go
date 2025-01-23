package partialdata

import (
	"errors"
)

type IsCfgEnabledFunc func(userID string) bool

const ErrorMsg string = "Query result may contain partial data."

type Error struct{}

func (e Error) Error() string {
	return ErrorMsg
}

func IsPartialDataError(err error) bool {
	return errors.As(err, &Error{})
}
