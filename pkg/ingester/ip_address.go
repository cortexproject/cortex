package ingester

import (
	"golang.org/x/net/context"
)

// extractIPAddress extracts the X-Forwarded-For header from the context
// It returns the empty string if the header is not set
func extractIPAddress(ctx context.Context) string {
	fwd, ok := ctx.Value("X-Forwarded-For").(string)
	if !ok {
		return ""
	}
	return fwd
}
