package memberlist

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func TestPage(t *testing.T) {
	require.NoError(t, pageTemplate.Execute(&bytes.Buffer{}, pageData{
		Now:           time.Now(),
		Initialized:   false,
		Memberlist:    nil,
		SortedMembers: nil,
		Store:         nil,
	}))

	conf := memberlist.DefaultLANConfig()
	ml, err := memberlist.Create(conf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ml.Shutdown()
	})

	require.NoError(t, pageTemplate.Execute(&bytes.Buffer{}, pageData{
		Now:           time.Now(),
		Initialized:   true,
		Memberlist:    ml,
		SortedMembers: ml.Members(),
		Store:         nil,
	}))
}
