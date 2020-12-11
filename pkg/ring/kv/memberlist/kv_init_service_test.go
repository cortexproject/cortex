package memberlist

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func TestPage(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	ml, err := memberlist.Create(conf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ml.Shutdown()
	})

	require.NoError(t, pageTemplate.Execute(&bytes.Buffer{}, pageData{
		Now:           time.Now(),
		Memberlist:    ml,
		SortedMembers: ml.Members(),
		Store:         nil,
		ReceivedMessages: []message{{
			ID:   10,
			Time: time.Now(),
			Size: 50,
			Pair: KeyValuePair{
				Key:   "hello",
				Value: []byte("world"),
				Codec: "coded",
			},
			Version: 20,
		}},

		SentMessages: []message{{
			ID:   10,
			Time: time.Now(),
			Size: 50,
			Pair: KeyValuePair{
				Key:   "hello",
				Value: []byte("world"),
				Codec: "coded",
			},
			Version: 20,
		}},
	}))
}
