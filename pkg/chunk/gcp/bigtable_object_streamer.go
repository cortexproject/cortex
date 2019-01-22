package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/cortexproject/cortex/pkg/chunk"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
)

// Stream forwards metrics to a golang channel, forwarded chunks must have the same
// user ID
func (b *bigtableStreamer) Stream(ctx context.Context, out chan []chunk.Chunk) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "Stream")
	defer sp.Finish()

	for _, query := range b.queries {
		err := b.streamQuery(ctx, query, out)
		if err != nil {
			return err
		}
		for len(out) != 0 {
			continue
		}
	}
	return nil
}

func (b *bigtableStreamer) streamQuery(ctx context.Context, query bigtableStreamQuery, out chan []chunk.Chunk) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "streamQuery")
	defer sp.Finish()
	sp.LogFields(otlog.String("id", query.id))

	var (
		table         = b.client.Open(query.table)
		processingErr error
	)

	decodeContext := chunk.NewDecodeContext()
	rr := query.generateRowRange()
	var chunks []chunk.Chunk
	var curKey string
	// Read through rows and forward slices of chunks with the same metrics
	// fingerprint
	err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
		c, err := chunk.ParseExternalKey(query.userID, row.Key())
		if err != nil {
			processingErr = err
			return false
		}
		if c.Fingerprint.String() != curKey {
			out <- chunks
			curKey = c.Fingerprint.String()
			chunks = []chunk.Chunk{}
		}
		err = c.Decode(decodeContext, row[columnFamily][0].Value)
		if err != nil {
			processingErr = err
			return false
		}
		chunks = append(chunks, c)
		return true
	})

	sp.LogFields(otlog.Int("chunks", len(chunks)))

	if err != nil {
		sp.LogFields(otlog.Error(err))
		return fmt.Errorf("stream canceled, %v. current query %v_%v, with user %v ", err, query.table, query.identifier, query.userID)
	}
	out <- chunks
	if processingErr != nil {
		sp.LogFields(otlog.Error(processingErr))
		return fmt.Errorf("stream canceled, %v. current query %v_%v, with user %v ", processingErr, query.table, query.identifier, query.userID)
	}

	return nil
}

func (b *bigtableStreamer) Size(ctx context.Context) (int, error) {
	n := 0
	for _, query := range b.queries {
		table := b.client.Open(query.table)
		rr := query.generateRowRange()

		err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
			n++
			return true
		}, bigtable.RowFilter(bigtable.StripValueFilter()))
		if err != nil {
			return 0, fmt.Errorf("error: %v, query: %v_%v_%v", err, query.table, query.identifier, query.userID)
		}
	}
	return n, nil
}

type bigtableStreamer struct {
	queries []bigtableStreamQuery
	client  *bigtable.Client
}

func (b *bigtableStreamer) Add(tableName, userID string, from, to int) {
	queryID := fmt.Sprintf("%v_%v_%v_%v", tableName, userID, from, to)
	if from == 1 && to == 240 {
		b.queries = append(b.queries, bigtableStreamQuery{queryID, userID, "", tableName})
		return
	}
	for i := from; i < to; i++ {
		prefix := fmt.Sprintf("%02x", i+16)
		b.queries = append(b.queries, bigtableStreamQuery{queryID, userID, prefix, tableName})
	}
}

type bigtableStreamQuery struct {
	id         string
	userID     string
	identifier string
	table      string
}

func (b bigtableStreamQuery) generateRowRange() bigtable.RowRange {
	return bigtable.PrefixRange(b.userID + "/" + b.identifier)
}
