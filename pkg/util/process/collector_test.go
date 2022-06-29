package process

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessCollector(t *testing.T) {
	const pid = 1

	// Create a mocked proc FS.
	procDir := t.TempDir()

	mapsPath := processMapsPath(procDir, pid)
	mapsLimitPath := vmMapsLimitPath(procDir)

	require.NoError(t, os.MkdirAll(filepath.Dir(mapsPath), os.ModePerm))
	require.NoError(t, os.MkdirAll(filepath.Dir(mapsLimitPath), os.ModePerm))

	require.NoError(t, ioutil.WriteFile(mapsPath, []byte("1\n2\n3\n4\n5\n"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(mapsLimitPath, []byte("262144\n"), os.ModePerm))

	// Create a new collector and test metrics.
	c, err := newProcessCollector(pid, procDir)
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP process_memory_map_areas Number of memory map areas allocated by the process.
		# TYPE process_memory_map_areas gauge
		process_memory_map_areas 5
		# HELP process_memory_map_areas_limit Maximum number of memory map ares the process can allocate.
		# TYPE process_memory_map_areas_limit gauge
		process_memory_map_areas_limit 262144
	`)))
}

func TestProcessCollector_UnsupportedPlatform(t *testing.T) {
	_, err := newProcessCollector(1, "/path/to/invalid/proc")
	require.Equal(t, ErrUnsupportedCollector, err)
}
