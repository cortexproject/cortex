package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/cortexproject/cortex/pkg/chunk"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
)

type scanner struct {
	client *bigtable.Client
}

// Scan forwards metrics to a golang channel, forwarded chunks must have the same
// user ID
func (s *scanner) Scan(ctx context.Context, req chunk.ScanRequest, out chan []chunk.Chunk) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "Stream")
	defer sp.Finish()

	query := generateBigtableScanQuery(req)
	sp.LogFields(otlog.String("id", query.id))

	var (
		table         = s.client.Open(query.table)
		processingErr error
	)

	decodeContext := chunk.NewDecodeContext()
	rr := query.generateRowRange()
	var chunks []chunk.Chunk
	var curKey string
	// Read through rows and forward slices of chunks with the same metrics
	// fingerprint
	err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
		c, err := chunk.ParseExternalKey(query.user, row.Key())
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
		return fmt.Errorf("stream canceled, %v. current query %v_%v, with user %v ", err, query.table, query.identifier, query.user)
	}
	out <- chunks
	if processingErr != nil {
		sp.LogFields(otlog.Error(processingErr))
		return fmt.Errorf("stream canceled, %v. current query %v_%v, with user %v ", processingErr, query.table, query.identifier, query.user)
	}

	return nil
}

func generateBigtableScanQuery(req chunk.ScanRequest) bigtableScanQuery {
	var id string
	var prefix string
	if req.Shard < 0 {
		id = fmt.Sprintf("%v_%v_all", req.Table, req.User)
		prefix = ""
	} else {
		id = fmt.Sprintf("%v_%v_%v", req.Table, req.User, req.Shard)
		prefix = fmt.Sprintf("%02x", req.Shard+15)
	}
	return bigtableScanQuery{id, req.User, prefix, req.Table}
}

type bigtableScanQuery struct {
	id         string
	user       string
	identifier string
	table      string
}

func (b bigtableScanQuery) generateRowRange() bigtable.RowRange {
	return bigtable.PrefixRange(b.user + "/" + b.identifier)
}
