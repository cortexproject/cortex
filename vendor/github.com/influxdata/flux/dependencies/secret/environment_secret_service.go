package secret

import (
	"context"
	"os"
)

func (ess EnvironmentSecretService) LoadSecret(ctx context.Context, k string) (string, error) {
	return os.Getenv(k), nil
}

// Secret service that retrieve the system environment variables.
type EnvironmentSecretService struct {
}
