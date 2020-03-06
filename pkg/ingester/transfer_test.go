package ingester

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	rnd "math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/shipper"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

type testUserTSDB struct {
	userID      string
	shipPercent int
	numBlocks   int
	meta        *shipper.Meta
	unshipped   []string
}

func createTSDB(t *testing.T, dir string, users []*testUserTSDB) {
	createAndWrite := func(t *testing.T, path string) {
		f, err := os.Create(path)
		require.NoError(t, err)
		defer f.Close()
		_, err = f.Write([]byte("a man a plan a canal panama"))
		require.NoError(t, err)
	}

	for _, user := range users {
		userDir := filepath.Join(dir, user.userID)

		err := os.MkdirAll(userDir, 0777)
		require.NoError(t, err)

		// Generate blocks.
		for i := 0; i < user.numBlocks; i++ {
			u, err := ulid.New(uint64(time.Now().Unix()*1000), rand.Reader)
			require.NoError(t, err)

			blockDir := filepath.Join(userDir, u.String())
			require.NoError(t, os.MkdirAll(filepath.Join(blockDir, "chunks"), 0777))

			for i := 0; i < 2; i++ {
				createAndWrite(t, filepath.Join(blockDir, "chunks", fmt.Sprintf("00000%v", i)))
			}

			meta := []string{"index", "meta.json", "tombstones"}
			for _, name := range meta {
				createAndWrite(t, filepath.Join(blockDir, name))
			}

			// Record if this block is to be "shipped"
			if rnd.Intn(100) < user.shipPercent {
				user.meta.Uploaded = append(user.meta.Uploaded, u)
			} else {
				user.unshipped = append(user.unshipped, u.String())
			}
		}

		// Generate WAL.
		require.NoError(t, os.MkdirAll(filepath.Join(userDir, "wal", "checkpoint.000419"), 0777))
		createAndWrite(t, filepath.Join(userDir, "wal", "000001"))
		createAndWrite(t, filepath.Join(userDir, "wal", "checkpoint.000419", "000000"))

		require.NoError(t, shipper.WriteMetaFile(nil, filepath.Join(dir, user.userID), user.meta))
	}
}

func TestUnshippedBlocks(t *testing.T) {
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)

	// Validate empty dir
	blks, err := unshippedBlocks(dir)
	require.NoError(t, err)
	require.Empty(t, blks)

	/*
		Create three user dirs
		One of them has some blocks shipped,
		One of them has all blocks shipped,
		One of them has no blocks shipped,
	*/
	users := []*testUserTSDB{
		{
			userID:      "0",
			shipPercent: 70,
			numBlocks:   10,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
			unshipped: []string{},
		},
		{
			userID:      "1",
			shipPercent: 100,
			numBlocks:   10,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
			unshipped: []string{},
		},
		{
			userID:      "2",
			shipPercent: 0,
			numBlocks:   10,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
			unshipped: []string{},
		},
	}

	createTSDB(t, dir, users)

	blks, err = unshippedBlocks(dir)
	require.NoError(t, err)
	for _, u := range users {
		_, ok := blks[u.userID]
		require.True(t, ok)
	}

	// Validate the unshipped blocks against the returned list
	for _, user := range users {
		require.ElementsMatch(t, user.unshipped, blks[user.userID])
	}
}

type MockTransferTSDBClient struct {
	Dir string

	grpc.ClientStream
}

func (m *MockTransferTSDBClient) Send(f *client.TimeSeriesFile) error {
	dir, _ := filepath.Split(f.Filename)
	if err := os.MkdirAll(filepath.Join(m.Dir, dir), 0777); err != nil {
		return err
	}
	if _, err := os.Create(filepath.Join(m.Dir, f.Filename)); err != nil {
		return err
	}
	return nil
}

func (m *MockTransferTSDBClient) CloseAndRecv() (*client.TransferTSDBResponse, error) {
	return &client.TransferTSDBResponse{}, nil
}

func (m *MockTransferTSDBClient) Context() context.Context {
	return context.Background()
}

func TestTransferUser(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	// Create an ingester without starting it (not needed).
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), reg)
	require.NoError(t, err)
	defer cleanup()

	// Create a fake TSDB on disk.
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)

	createTSDB(t, dir, []*testUserTSDB{
		{
			userID:      "0",
			shipPercent: 0,
			numBlocks:   3,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
		},
	})

	blks, err := unshippedBlocks(dir)
	require.NoError(t, err)

	xfer, err := ioutil.TempDir("", "xfer")
	require.NoError(t, err)
	m := &MockTransferTSDBClient{
		Dir: xfer,
	}
	i.transferUser(context.Background(), m, dir, "test", "0", blks["0"])

	var original []string
	var xferfiles []string
	err = filepath.Walk(xfer, func(path string, info os.FileInfo, err error) error {
		p, _ := filepath.Rel(xfer, path)
		xferfiles = append(xferfiles, p)
		return nil
	})
	require.NoError(t, err)

	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.Name() == "thanos.shipper.json" {
			return nil
		}
		p, _ := filepath.Rel(dir, path)
		original = append(original, p)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, original, xferfiles)

	// Assert exported metrics (3 blocks, 5 files per block, 2 files WAL).
	metricNames := []string{
		"cortex_ingester_sent_files",
		"cortex_ingester_received_files",
		"cortex_ingester_received_bytes_total",
		"cortex_ingester_sent_bytes_total",
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_sent_files The total number of files sent by this ingester whilst leaving.
		# TYPE cortex_ingester_sent_files counter
		cortex_ingester_sent_files 17
		# HELP cortex_ingester_received_files The total number of files received by this ingester whilst joining
		# TYPE cortex_ingester_received_files counter
		cortex_ingester_received_files 0
		# HELP cortex_ingester_received_bytes_total The total number of bytes received by this ingester whilst joining
		# TYPE cortex_ingester_received_bytes_total counter
		cortex_ingester_received_bytes_total 0
		# HELP cortex_ingester_sent_bytes_total The total number of bytes sent by this ingester whilst leaving
		# TYPE cortex_ingester_sent_bytes_total counter
		cortex_ingester_sent_bytes_total 459
	`), metricNames...))
}
