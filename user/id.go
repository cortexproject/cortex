package user

import (
	"fmt"

	"golang.org/x/net/context"
)

// UserIDContextKey is the key used in contexts to find the userid
const userIDContextKey = "CortexUserID" // TODO dedupe with storage/local

// GetID returns the user
func GetID(ctx context.Context) (string, error) {
	userid, ok := ctx.Value(userIDContextKey).(string)
	if !ok {
		return "", fmt.Errorf("no user id")
	}
	return userid, nil
}

// WithID returns a derived context containing the user ID.
func WithID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, interface{}(userIDContextKey), userID)
}
