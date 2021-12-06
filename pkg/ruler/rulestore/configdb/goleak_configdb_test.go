package configdb

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	if os.Getenv("CORTEX_TEST_GOLEAK") == "1" {
		goleak.VerifyTestMain(m)
	}
}
