package storage

import (
	"os"
	"testing"

	"github.com/go-kit/log"
)

func TestDisplay(t *testing.T) {
	stats := NewStats()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	extraFields := []interface{}{
		"request", "labelNames",
		"mint", 0,
		"maxt", 100000,
	}
	stats.AddColumnSearched("name", 1000)
	stats.AddPagesSearchSizeBytes("job", 100000)
	stats.Display(logger, extraFields...)
}
