package grpc

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

func (w *WriteBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	w.Writes = append(w.Writes, &IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

func (w *WriteBatch) Delete(tableName, hashValue string, rangeValue []byte) {
	w.Deletes = append(w.Deletes, &IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
	})
}

func (s *StorageClient) NewWriteBatch() chunk.WriteBatch {
	return &WriteBatch{}
}

func (s *StorageClient) BatchWrite(c context.Context, batch chunk.WriteBatch) error {
	writeBatch := batch.(*WriteBatch)
	batchWrites := &BatchWrites{Writes: writeBatch.Writes}
	_, err := s.client.BatchWrite(context.Background(), batchWrites)
	if err != nil {
		return errors.WithStack(err)
	}

	batchDeletes := &BatchDeletes{Deletes: writeBatch.Deletes}
	_, err = s.client.Delete(context.Background(), batchDeletes)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *StorageClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return util.DoParallelQueries(ctx, s.query, queries, callback)
}

func (s *StorageClient) query(ctx context.Context, query chunk.IndexQuery, callback util.Callback) error {
	indexQuery := &IndexQuery{
		TableName:        query.TableName,
		HashValue:        query.HashValue,
		RangeValuePrefix: query.RangeValuePrefix,
		RangeValueStart:  query.RangeValueStart,
		ValueEqual:       query.ValueEqual,
		Immutable:        query.Immutable,
	}
	streamer, err := s.client.QueryPages(ctx, indexQuery)
	if err != nil {
		return errors.WithStack(err)
	}
	for {
		readBatch, err := streamer.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if !callback(query, readBatch) {
			return nil
		}
	}

	return nil
}

func (r *ReadBatch) Iterator() chunk.ReadBatchIterator {
	return &grpcIter{
		i:         -1,
		ReadBatch: r,
	}
}

type grpcIter struct {
	i int
	*ReadBatch
}

func (b *grpcIter) Next() bool {
	b.i++
	return b.i < len(b.Rows)
}

func (b *grpcIter) RangeValue() []byte {
	return b.Rows[b.i].RangeValue
}

func (b *grpcIter) Value() []byte {
	return b.Rows[b.i].Value
}
