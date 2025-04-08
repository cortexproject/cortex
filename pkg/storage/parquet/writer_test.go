package storage

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestWriteColumnIstNotFound(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	cols := []string{"country", "stats"}
	w := NewParquetWriter(buffer, 100, 100, 3, cols, "")

	err := w.WriteRows([]ParquetRow{{Columns: map[string]string{"not_defined": ""}}})
	require.Error(t, err)

	err = w.WriteRows([]ParquetRow{{Columns: map[string]string{"country": "brazil"}}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := parquet.Read[any](BufferReadAt{buffer: buffer}, int64(buffer.Len()), w.w.Schema())
	require.NoError(t, err)
	require.Len(t, r, 1)
	require.Equal(t, r[0].(map[string]any)["country"], "brazil")
}

func TestWriteColumnInsertOutOfOrder(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	cols := []string{"country", "stats"}
	w := NewParquetWriter(buffer, 100, 100, 3, cols, "country")

	err := w.WriteRows([]ParquetRow{{Columns: map[string]string{"country": "brazil"}}})
	require.NoError(t, err)

	err = w.WriteRows([]ParquetRow{{Columns: map[string]string{"country": "zimbabue"}}})
	require.NoError(t, err)

	// Out of order insert
	err = w.WriteRows([]ParquetRow{{Columns: map[string]string{"country": "columbia"}}})
	require.Error(t, err)
	require.NoError(t, w.Close())

	r, err := parquet.Read[any](BufferReadAt{buffer: buffer}, int64(buffer.Len()), w.w.Schema())
	require.NoError(t, err)
	require.Len(t, r, 2)
	require.Equal(t, r[0].(map[string]any)["country"], "brazil")
	require.Equal(t, r[1].(map[string]any)["country"], "zimbabue")
}
