package partialdata

import (
	"errors"
)

const ErrorMsg string = "Query result may contain partial data."

type Error struct{}

func (e Error) Error() string {
	return ErrorMsg
}

func ReturnPartialData(err error, isEnabled bool) bool {
	return isEnabled && errors.As(err, &Error{})
}
