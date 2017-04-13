package chunk

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/test"
)

func TestSchemaComposite(t *testing.T) {
	type result struct {
		from, through model.Time
		schema        Schema
	}
	collect := func(results *[]result) func(from, through model.Time, schema Schema) ([]IndexEntry, error) {
		return func(from, through model.Time, schema Schema) ([]IndexEntry, error) {
			*results = append(*results, result{from, through, schema})
			return nil, nil
		}
	}
	cs := compositeSchema{
		schemas: []compositeSchemaEntry{
			{model.TimeFromUnix(0), mockSchema(1)},
			{model.TimeFromUnix(100), mockSchema(2)},
			{model.TimeFromUnix(200), mockSchema(3)},
		},
	}

	for i, tc := range []struct {
		cs            compositeSchema
		from, through int64
		want          []result
	}{
		// Test we have sensible results when there are no schema's defined
		{compositeSchema{}, 0, 1, []result{}},

		// Test we have sensible results when there is a single schema
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{model.TimeFromUnix(0), mockSchema(1)},
				},
			},
			0, 10,
			[]result{
				{model.TimeFromUnix(0), model.TimeFromUnix(10), mockSchema(1)},
			},
		},

		// Test we have sensible results for negative (ie pre 1970) times
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{model.TimeFromUnix(0), mockSchema(1)},
				},
			},
			-10, -9,
			[]result{},
		},
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{model.TimeFromUnix(0), mockSchema(1)},
				},
			},
			-10, 10,
			[]result{
				{model.TimeFromUnix(0), model.TimeFromUnix(10), mockSchema(1)},
			},
		},

		// Test we have sensible results when there is two schemas
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{model.TimeFromUnix(0), mockSchema(1)},
					{model.TimeFromUnix(100), mockSchema(2)},
				},
			},
			34, 165,
			[]result{
				{model.TimeFromUnix(34), model.TimeFromUnix(100) - 1, mockSchema(1)},
				{model.TimeFromUnix(100), model.TimeFromUnix(165), mockSchema(2)},
			},
		},

		// Test we get only one result when two schema start at same time
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{model.TimeFromUnix(0), mockSchema(1)},
					{model.TimeFromUnix(10), mockSchema(2)},
					{model.TimeFromUnix(10), mockSchema(3)},
				},
			},
			0, 165,
			[]result{
				{model.TimeFromUnix(0), model.TimeFromUnix(10) - 1, mockSchema(1)},
				{model.TimeFromUnix(10), model.TimeFromUnix(165), mockSchema(3)},
			},
		},

		// Test all the various combination we can get when there are three schemas
		{
			cs, 34, 65,
			[]result{
				{model.TimeFromUnix(34), model.TimeFromUnix(65), mockSchema(1)},
			},
		},

		{
			cs, 244, 6785,
			[]result{
				{model.TimeFromUnix(244), model.TimeFromUnix(6785), mockSchema(3)},
			},
		},

		{
			cs, 34, 165,
			[]result{
				{model.TimeFromUnix(34), model.TimeFromUnix(100) - 1, mockSchema(1)},
				{model.TimeFromUnix(100), model.TimeFromUnix(165), mockSchema(2)},
			},
		},

		{
			cs, 151, 264,
			[]result{
				{model.TimeFromUnix(151), model.TimeFromUnix(200) - 1, mockSchema(2)},
				{model.TimeFromUnix(200), model.TimeFromUnix(264), mockSchema(3)},
			},
		},

		{
			cs, 32, 264,
			[]result{
				{model.TimeFromUnix(32), model.TimeFromUnix(100) - 1, mockSchema(1)},
				{model.TimeFromUnix(100), model.TimeFromUnix(200) - 1, mockSchema(2)},
				{model.TimeFromUnix(200), model.TimeFromUnix(264), mockSchema(3)},
			},
		},
	} {
		t.Run(fmt.Sprintf("TestSchemaComposite[%d]", i), func(t *testing.T) {
			have := []result{}
			tc.cs.forSchemas(model.TimeFromUnix(tc.from), model.TimeFromUnix(tc.through), collect(&have))
			if !reflect.DeepEqual(tc.want, have) {
				t.Fatalf("wrong schemas - %s", test.Diff(tc.want, have))
			}
		})
	}
}

func TestSchemaDailyBuckets(t *testing.T) {
	const (
		userID     = "0"
		metricName = model.LabelValue("name")
		tableName  = "table"
	)
	var (
		config = SchemaConfig{
			OriginalTableName: tableName,
		}
	)

	for _, c := range []struct {
		from, through model.Time
	}{
		{
			from:    model.TimeFromUnix(0),
			through: model.TimeFromUnix(2 * 24 * 3600),
		},
	} {
		var i int64
		_, err := config.dailyBuckets(c.from, c.through, userID, metricName, func(from, through uint32, tableName, hashKey string) ([]IndexEntry, error) {
			require.True(t, (i*millisecondsInDay)+int64(from) >= int64(c.from), "%d <= %d", (i*millisecondsInDay)+int64(from), int64(c.from))
			require.True(t, (i*millisecondsInDay)+int64(from) <= int64(c.through), "%d >= %d", (i*millisecondsInDay)+int64(from), int64(c.through))
			require.True(t, (i*millisecondsInDay)+int64(through) >= int64(c.from), "%d <= %d", (i*millisecondsInDay)+int64(through), int64(c.from))
			require.True(t, (i*millisecondsInDay)+int64(through) <= int64(c.through), "%d >= %d", (i*millisecondsInDay)+int64(through), int64(c.through))
			i++
			return nil, nil
		})
		assert.NoError(t, err)
	}
}
