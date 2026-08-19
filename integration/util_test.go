//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLatestReleaseVersion(t *testing.T) {
	tests := map[string]struct {
		version     string
		expected    string
		expectedErr bool
	}{
		"a GA version is already published": {
			version:  "1.21.1",
			expected: "1.21.1",
		},
		"a GA version with a zero patch is already published": {
			version:  "1.21.0",
			expected: "1.21.0",
		},
		"a minor release candidate falls back to the previous minor": {
			version:  "1.22.0-rc.0",
			expected: "1.21.0",
		},
		"a later minor release candidate falls back to the same previous minor": {
			version:  "1.22.0-rc.3",
			expected: "1.21.0",
		},
		"a patch release candidate falls back to the preceding patch": {
			version:  "1.22.1-rc.0",
			expected: "1.22.0",
		},
		"a later patch release candidate falls back to the preceding patch": {
			version:  "1.22.3-rc.1",
			expected: "1.22.2",
		},
		"a major release candidate cannot be resolved": {
			version:     "2.0.0-rc.0",
			expectedErr: true,
		},
		"an empty VERSION is rejected": {
			version:     "",
			expectedErr: true,
		},
		"a malformed pre-release base is rejected": {
			version:     "1.22-rc.0",
			expectedErr: true,
		},
		"a non-numeric pre-release base is rejected": {
			version:     "1.x.0-rc.0",
			expectedErr: true,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := latestReleaseVersion(testData.version)
			if testData.expectedErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestGetLatestReleaseImage(t *testing.T) {
	// Point getCortexProjectDir() at a scratch checkout so we can exercise the VERSION file
	// contents a release branch would actually have.
	dir := t.TempDir()
	t.Setenv("CORTEX_CHECKOUT_DIR", dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "VERSION"), []byte("1.22.0-rc.0\n"), 0o600))

	image, err := getLatestReleaseImage()
	require.NoError(t, err)
	assert.Equal(t, "quay.io/cortexproject/cortex:v1.21.0", image)
}

func TestGetLatestReleaseImage_HonorsOverride(t *testing.T) {
	t.Setenv("CORTEX_LATEST_RELEASE_IMAGE", "quay.io/cortexproject/cortex:v1.20.1")

	image, err := getLatestReleaseImage()
	require.NoError(t, err)
	assert.Equal(t, "quay.io/cortexproject/cortex:v1.20.1", image)
}
