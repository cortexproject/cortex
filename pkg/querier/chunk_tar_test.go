package querier

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestChunkTar(t *testing.T) {
	chunksFilename := os.Getenv("CHUNKS")
	if len(chunksFilename) == 0 {
		return
	}

	query := os.Getenv("QUERY")
	if len(query) == 0 {
		return
	}

	userID := os.Getenv("USERID")
	if len(query) == 0 {
		return
	}

	from, err := parseTime(os.Getenv("FROM"))
	require.NoError(t, err)

	through, err := parseTime(os.Getenv("THROUGH"))
	require.NoError(t, err)

	step, err := parseDuration(os.Getenv("STEP"))
	require.NoError(t, err)

	chunks, err := loadChunks(userID, chunksFilename)
	require.NoError(t, err)

	store := mockChunkStore{chunks}
	queryable := newChunkStoreQueryable(store, batch.NewChunkMergeIterator)
	engine := promql.NewEngine(util.Logger, nil, 10, 1*time.Minute)
	rangeQuery, err := engine.NewRangeQuery(queryable, query, from, through, step)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "0")
	r := rangeQuery.Exec(ctx)
	_, err = r.Matrix()
	require.NoError(t, err)
}

func parseTime(s string) (time.Time, error) {
	t, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, err
	}
	secs, ns := math.Modf(t)
	tm := time.Unix(int64(secs), int64(ns*float64(time.Second)))
	return tm, nil
}

func parseDuration(s string) (time.Duration, error) {
	t, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Duration(0), err
	}
	return time.Duration(t * float64(time.Second)), nil
}

func loadChunks(userID, filename string) ([]chunk.Chunk, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	var chunks []chunk.Chunk
	tarReader := tar.NewReader(gzipReader)
	ctx := chunk.NewDecodeContext()
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "here 1")
		}

		c, err := chunk.ParseExternalKey(userID, hdr.Name)
		if err != nil {
			return nil, errors.Wrap(err, "here 2")
		}

		fmt.Println(hdr.Name)

		var buf = make([]byte, int(hdr.Size))
		if _, err := io.ReadFull(tarReader, buf); err != nil {
			return nil, errors.Wrap(err, "here 3")
		}

		if err := c.Decode(ctx, buf); err != nil {
			return nil, errors.Wrap(err, "here 4")
		}

		chunks = append(chunks, c)
	}

	return chunks, nil
}
