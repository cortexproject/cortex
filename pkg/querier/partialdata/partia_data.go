package partialdata

import (
	"context"
	"errors"
)

type partialDataCtxKey struct{}

var (
	ctxKey = &partialDataCtxKey{}
)

const ErrorMsg string = "Query result may contain partial data."

type Error struct{}

func (e Error) Error() string {
	return ErrorMsg
}

func ContextWithPartialData(ctx context.Context, isEnabled bool) context.Context {
	if isEnabled {
		return context.WithValue(ctx, ctxKey, isEnabled)
	}
	return ctx
}

func FromContext(ctx context.Context) bool {
	o := ctx.Value(ctxKey)
	if o == nil {
		return false
	}
	return o.(bool)
}

func ReturnPartialData(err error, isEnabled bool) bool {
	return isEnabled && errors.As(err, &Error{})
}
