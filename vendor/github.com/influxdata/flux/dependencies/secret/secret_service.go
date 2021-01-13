package secret

import (
	"context"
)

// Service generalizes the process of looking up secrets based on a key.
type Service interface {
	// LoadSecret retrieves the secret value v found at key k given the calling context ctx.
	LoadSecret(ctx context.Context, k string) (string, error)
}
