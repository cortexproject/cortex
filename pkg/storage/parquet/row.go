package storage

const (
	ColHash                        = "s_hash"
	ColIndexes                     = "s_col_indexes"
	ColData                        = "s_data"
	SortedColMetadataKey           = "sorting_col_metadata_key"
	NumberOfDataColumnsMetadataKey = "number_of_data_columns_metadata_key"
	IndexSizeLimitMetadataKey      = "index_size_limit_metadata_key"
)

type ParquetRow struct {
	Hash    uint64
	Data    [][]byte
	Columns map[string]string
}
