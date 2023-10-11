package compactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

func TestPreCompactionCallback(t *testing.T) {
	compactDir, err := os.MkdirTemp(os.TempDir(), "compact")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(compactDir))
	})

	lifecycleCallback := ShardedCompactionLifecycleCallback{
		compactDir: compactDir,
	}

	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	meta := []*metadata.Meta{
		{
			BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
		},
		{
			BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
		},
		{
			BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
		},
	}
	testGroupKey := "test_group_key"
	testGroup, _ := compact.NewGroup(
		log.NewNopLogger(),
		nil,
		testGroupKey,
		nil,
		0,
		true,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		metadata.NoneFunc,
		1,
		1,
	)
	for _, m := range meta {
		err := testGroup.AppendMeta(m)
		require.NoError(t, err)
	}

	dummyGroupID1 := "dummy_dir_1"
	dummyGroupID2 := "dummy_dir_2"
	err = os.MkdirAll(filepath.Join(compactDir, testGroupKey), 0750)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(compactDir, testGroupKey, block1.String()), 0750)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(compactDir, dummyGroupID1), 0750)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(compactDir, dummyGroupID2), 0750)
	require.NoError(t, err)

	err = lifecycleCallback.PreCompactionCallback(context.Background(), log.NewNopLogger(), testGroup, meta)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(compactDir, testGroupKey))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	info, err = os.Stat(filepath.Join(compactDir, testGroupKey, block1.String()))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	_, err = os.Stat(filepath.Join(compactDir, dummyGroupID1))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(compactDir, dummyGroupID2))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}
