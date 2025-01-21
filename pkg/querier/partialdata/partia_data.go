package partialdata

import (
	"context"
	"errors"
)

const (
	partialDataCtxKey   string = "partialDataCtxKey"
	partialDataErrorMsg string = "Query result may contain partial data."
)

type Error struct{}

func (e Error) Error() string {
	return partialDataErrorMsg
}

func ContextWithPartialData(ctx context.Context, isEnabled bool) context.Context {
	if isEnabled {
		return context.WithValue(ctx, partialDataCtxKey, isEnabled)
	}
	return ctx
}

func FromContext(ctx context.Context) bool {
	o := ctx.Value(partialDataCtxKey)
	if o == nil {
		return false
	}
	return o.(bool)
}

func ReturnPartialData(err error, isEnabled bool) bool {
	return isEnabled && errors.As(err, &Error{})
}
