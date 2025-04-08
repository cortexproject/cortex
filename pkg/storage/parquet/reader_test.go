package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestSearchRows(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	cols := []string{"sorted_key", "col_0", "col_1", "col_2", "unique"}
	w := NewParquetWriter(buffer, 200, 100, 3, cols, "sorted_key")

	numberOfRows := 10000
	numberOfKeys := 500
	rows := generateRows("sorted_key", 3, numberOfRows, numberOfKeys)

	err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := NewParquetReader(BufferReadAt{buffer: buffer}.CreateReadAtWithContext, int64(buffer.Len()), false, NewCacheMetrics(prometheus.NewPedanticRegistry()), 10, nil, nil)
	require.NoError(t, err)
	sts := NewStats()

	// should not allow search rows on cols not sorted
	_, err = r.SearchRows(context.Background(), "col_1", "val", sts)
	require.Error(t, err)

	// searching sorted key
	allFounds := make([]ParquetRow, 0, len(rows))
	for i := 0; i < numberOfKeys; i++ {
		rows, err := r.SearchRows(context.Background(), "sorted_key", fmt.Sprintf("key_%d", i), sts)
		require.NoError(t, err)
		require.Equal(t, numberOfRows/numberOfKeys, countResults(rows))
		founds, err := r.Materialize(context.Background(), rows, []int{0, 1, 2}, nil, false, sts)
		require.NoError(t, err)
		require.Len(t, founds, numberOfRows/numberOfKeys)
		allFounds = append(allFounds, founds...)
	}

	require.Equal(t, len(rows), len(allFounds))
	for _, found := range allFounds {
		require.Contains(t, rows, found)
	}
}

func TestScanRows(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	cols := []string{"country", "stats", "indices"}

	countries := []string{"brazil", "canada", "equator", "japan"}
	stats := []string{"max", "min", "avg"}
	indices := []string{"population", "size"}

	numberOfExtraCountries := 1000

	for i := 0; i < numberOfExtraCountries; i++ {
		countries = append(countries, fmt.Sprintf("country_%d", i))
	}
	rows := make([]ParquetRow, 0, len(countries))

	for _, country := range countries {
		for _, stat := range stats {
			for _, ind := range indices {
				rows = append(rows, ParquetRow{
					Hash: uint64(rand.Int63()),
					Data: [][]byte{[]byte(fmt.Sprintf("%v_%v_%v", country, stat, ind))},
					Columns: map[string]string{
						"country": country,
						"stats":   stat,
						"indices": ind,
					},
				})
			}
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Columns["country"] < rows[j].Columns["country"]
	})

	w := NewParquetWriter(buffer, 200, 100, 3, cols, "country")
	err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := NewParquetReader(BufferReadAt{buffer: buffer}.CreateReadAtWithContext, int64(buffer.Len()), false, NewCacheMetrics(prometheus.NewPedanticRegistry()), 10, nil, nil)
	require.NoError(t, err)

	sts := NewStats()

	// should not allow search rows not sorted
	foundRows, err := r.SearchRows(context.Background(), "country", "brazil", sts)
	require.NoError(t, err)
	result, err := r.Materialize(context.Background(), foundRows, []int{0, 1, 2}, nil, false, sts)
	require.NoError(t, err)
	require.Len(t, result, len(stats)*len(indices))
	for _, r := range result {
		require.Equal(t, "brazil", r.Columns["country"])
	}

	// Scan for stats max
	foundRows, err = r.ScanRows(context.Background(), foundRows, false, labels.MustNewMatcher(labels.MatchEqual, "stats", "max"), sts)
	require.NoError(t, err)
	result, err = r.Materialize(context.Background(), foundRows, []int{0, 1, 2}, nil, false, sts)
	require.NoError(t, err)
	require.Len(t, result, len(indices))

	for _, r := range result {
		require.Equal(t, "brazil", r.Columns["country"])
		require.Equal(t, "max", r.Columns["stats"])
	}

	// Scan for indices:population
	foundRows, err = r.ScanRows(context.Background(), foundRows, false, labels.MustNewMatcher(labels.MatchEqual, "indices", "population"), sts)
	require.NoError(t, err)
	result, err = r.Materialize(context.Background(), foundRows, []int{0, 1, 2}, nil, false, sts)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "population", result[0].Columns["indices"])
	require.Equal(t, "brazil_max_population", string(result[0].Data[0]))

	// Lets create a full scan
	foundRows, err = r.ScanRows(context.Background(), foundRows, true, labels.MustNewMatcher(labels.MatchEqual, "indices", "population"), sts)
	require.NoError(t, err)
	result, err = r.Materialize(context.Background(), foundRows, []int{0, 1, 2}, nil, false, sts)
	require.NoError(t, err)
	require.Len(t, result, len(countries)*len(stats))
	for _, r := range result {
		require.Equal(t, "population", r.Columns["indices"])
	}

	foundRows, err = r.ScanRows(context.Background(), foundRows, false, labels.MustNewMatcher(labels.MatchEqual, "stats", "max"), sts)
	require.NoError(t, err)
	result, err = r.Materialize(context.Background(), foundRows, []int{0, 1, 2}, nil, false, sts)
	require.NoError(t, err)
	require.Len(t, result, len(countries))
	allCountriesFromResults := map[string]int{}

	for _, r := range result {
		require.Equal(t, "max", r.Columns["stats"])
		allCountriesFromResults[r.Columns["country"]]++
	}

	require.Equal(t, len(countries), len(allCountriesFromResults))

	// Should return empty for cols that does not exits
	foundRows, err = r.ScanRows(context.Background(), foundRows, true, labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), sts)
	require.NoError(t, err)
	result, err = r.Materialize(context.Background(), foundRows, []int{0, 1, 2}, nil, false, sts)
	require.NoError(t, err)
	require.Len(t, result, 0)
}

func generateRows(sortedKey string, numberOfCols, numberOfRows int, numberOfKeys int) []ParquetRow {
	var rows []ParquetRow
	for i := 0; i < numberOfRows; i++ {
		rows = append(rows, ParquetRow{
			Hash: uint64(int64(i)),
			Data: [][]byte{[]byte("oi")},
			Columns: map[string]string{
				sortedKey:                             fmt.Sprintf("key_%d", i%numberOfKeys),
				"unique":                              fmt.Sprintf("unique_%d", i),
				fmt.Sprintf("col_%v", i%numberOfCols): fmt.Sprintf("col_%v", i%numberOfCols),
			},
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Columns[sortedKey] < rows[j].Columns[sortedKey]
	})
	return rows
}

func countResults(r [][]int64) int {
	count := 0
	for _, row := range r {
		count += len(row)
	}
	return count
}
