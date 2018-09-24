package chunk

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestWriter(t *testing.T) {
	now := model.Now()
	fooChunk1, _, _, _ := dummyChunks(now)

	st := newTestChunkStore(t, "v6")
	defer st.Stop()
	store := st.(CompositeStore).stores[0].Store.(*store)

	var cfg WriterConfig
	util.DefaultValues(&cfg)
	writer := NewWriter(cfg, store.storage)
	writer.Run()

	writeReqs, err := store.calculateIndexEntries(userID, fooChunk1.From, fooChunk1.Through, fooChunk1)
	require.NoError(t, err)
	writer.Write <- writeReqs

	writer.Stop()
}
