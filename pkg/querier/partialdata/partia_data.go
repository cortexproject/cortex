package partialdata

import (
	"errors"
)

const ErrorMsg string = "Query result may contain partial data."

type Error struct{}

func (e Error) Error() string {
	return ErrorMsg
}

func IsPartialDataError(err error) bool {
	return errors.As(err, &Error{})
}
