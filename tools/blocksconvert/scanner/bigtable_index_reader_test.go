package scanner

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
)

func TestParseRowKey(t *testing.T) {
	tcs := map[string]struct {
		row   bigtable.Row
		table string

		expectedEntries []chunk.IndexEntry
		expectedError   string
	}{
		"newStorageClientV1 format": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "testUser:d18500:test_metric\u0000eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\u0000\u0000\u00007\u0000", Column: "f:c", Value: []byte("-")},
				},
			},
			table: "test",
			expectedEntries: []chunk.IndexEntry{
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:test_metric",
					RangeValue: []byte("eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\x00\x00\x007\x00"),
					Value:      []byte("-"),
				},
			},
		},
		// 2a) newStorageClientColumnKey, WITHOUT key distribution
		//    - RowKey = entry.HashValue
		//    - Column: entry.RangeValue (in family "f")
		//    - Value: entry.Value
		"newStorageClientColumnKey without key distribution": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "testUser:d18500:test_metric", Column: "f:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\u0000\u0000\u00007\u0000", Value: []byte("-")},
				},
			},
			table: "test",
			expectedEntries: []chunk.IndexEntry{
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:test_metric",
					RangeValue: []byte("eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\x00\x00\x007\x00"),
					Value:      []byte("-"),
				},
			},
		},
		"newStorageClientColumnKey without key distribution, multiple columns": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:00a4cb80\u0000\u0000chunkID_1\u00003\u0000", Value: []byte("")},
					{Row: "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:05265c00\x00\x00chunkID_2\x003\x00", Value: []byte("")},
					{Row: "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:0036ee80\x00\x00chunkID_2\x003\x00", Value: []byte("")},
				},
			},
			table: "test",
			expectedEntries: []chunk.IndexEntry{
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("0036ee80\x00\x00chunkID_2\x003\x00"),
					Value:      []byte(""),
				},
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("00a4cb80\x00\x00chunkID_1\x003\x00"),
					Value:      []byte(""),
				},
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("05265c00\x00\x00chunkID_2\x003\x00"),
					Value:      []byte(""),
				},
			},
		},

		// 2b) newStorageClientColumnKey, WITH key distribution
		//    - RowKey: hashPrefix(entry.HashValue) + "-" + entry.HashValue, where hashPrefix is 64-bit FNV64a hash, encoded as little-endian hex value
		//    - Column: entry.RangeValue (in family "f")
		//    - Value: entry.Value
		"newStorageClientColumnKey with key distribution, multiple columns": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "15820f698f0f8d81-testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:00a4cb80\u0000\u0000chunkID_1\u00003\u0000", Value: []byte("")},
					{Row: "15820f698f0f8d81-testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:05265c00\x00\x00chunkID_2\x003\x00", Value: []byte("")},
					{Row: "15820f698f0f8d81-testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:0036ee80\x00\x00chunkID_2\x003\x00", Value: []byte("")},
				},
			},
			table: "test",
			expectedEntries: []chunk.IndexEntry{
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("0036ee80\x00\x00chunkID_2\x003\x00"),
					Value:      []byte(""),
				},
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("00a4cb80\x00\x00chunkID_1\x003\x00"),
					Value:      []byte(""),
				},
				{
					TableName:  "test",
					HashValue:  "testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("05265c00\x00\x00chunkID_2\x003\x00"),
					Value:      []byte(""),
				},
			},
		},
		"different row keys": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "a", Column: "f:c", Value: []byte("-")},
					{Row: "b", Column: "f:c", Value: []byte("-")},
				},
			},
			table:         "test",
			expectedError: "rowkey mismatch: \"b\", \"a\"",
		},

		"newStorageClientV1, invalid column": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "testUser:d18500:test_metric\u0000eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\u0000\u0000\u00007\u0000", Column: "wrong", Value: []byte("-")},
				},
			},
			table:         "test",
			expectedError: "found rangeValue in RowKey, but column is not 'f:c': \"wrong\"",
		},

		"newStorageClientColumnKey, invalid column family": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "testUser:d18500:test_metric", Column: "family:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\u0000\u0000\u00007\u0000", Value: []byte("-")},
				},
			},
			table:         "test",
			expectedError: "invalid column prefix: \"family:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M\\x00\\x00\\x007\\x00\"",
		},
		"newStorageClientColumnKey, invalid hash (hash ignored, not stripped)": {
			row: map[string][]bigtable.ReadItem{
				"f": {
					{Row: "1234567890123456-testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M", Column: "f:00a4cb80\u0000\u0000chunkID_1\u00003\u0000", Value: []byte("")},
				},
			},
			expectedEntries: []chunk.IndexEntry{
				{
					TableName:  "test",
					HashValue:  "1234567890123456-testUser:d18500:eg856WuFz2TNSApvcW7LrhiPKgkuU6KfI3nJPwLoA0M",
					RangeValue: []byte("00a4cb80\u0000\u0000chunkID_1\u00003\u0000"),
					Value:      []byte(""),
				},
			},
			table: "test",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			entries, err := parseRowKey(tc.row, tc.table)

			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
				require.Nil(t, entries)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedEntries, entries)
			}
		})
	}
}
