package ingester

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestGlobalLimitsUnmarshal(t *testing.T) {
	defaultGlobalLimits = &GlobalLimits{
		MaxIngestionRate:        10,
		MaxInMemoryUsers:        20,
		MaxInMemorySeries:       30,
		MaxInflightPushRequests: 40,
	}

	l := GlobalLimits{}
	input := `
max_ingestion_rate: 125.678
max_users: 50000
`

	require.NoError(t, yaml.UnmarshalStrict([]byte(input), &l))
	require.Equal(t, float64(125.678), l.MaxIngestionRate)
	require.Equal(t, int64(50000), l.MaxInMemoryUsers)
	require.Equal(t, int64(30), l.MaxInMemorySeries)       // default value
	require.Equal(t, int64(40), l.MaxInflightPushRequests) // default value
}
