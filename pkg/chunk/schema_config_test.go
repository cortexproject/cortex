package chunk

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHourlyBuckets(t *testing.T) {
	const (
		userID     = "0"
		metricName = model.LabelValue("name")
		tableName  = "table"
	)
	var cfg = PeriodConfig{
		IndexTables: PeriodicTableConfig{Prefix: tableName},
	}

	type args struct {
		from    model.Time
		through model.Time
	}
	tests := []struct {
		name string
		args args
		want []Bucket
	}{
		{
			"0 hour window",
			args{
				from:    model.TimeFromUnix(0),
				through: model.TimeFromUnix(0),
			},
			[]Bucket{{
				from:      0,
				through:   0,
				tableName: "table",
				hashKey:   "0:0",
			}},
		},
		{
			"30 minute window",
			args{
				from:    model.TimeFromUnix(0),
				through: model.TimeFromUnix(1800),
			},
			[]Bucket{{
				from:      0,
				through:   1800 * 1000, // ms
				tableName: "table",
				hashKey:   "0:0",
			}},
		},
		{
			"1 hour window",
			args{
				from:    model.TimeFromUnix(0),
				through: model.TimeFromUnix(3600),
			},
			[]Bucket{{
				from:      0,
				through:   3600 * 1000, // ms
				tableName: "table",
				hashKey:   "0:0",
			}, {
				from:      0,
				through:   0, // ms
				tableName: "table",
				hashKey:   "0:1",
			}},
		},
		{
			"window spanning 3 hours with non-zero start",
			args{
				from:    model.TimeFromUnix(900),
				through: model.TimeFromUnix((2 * 3600) + 1800),
			},
			[]Bucket{{
				from:      900 * 1000,  // ms
				through:   3600 * 1000, // ms
				tableName: "table",
				hashKey:   "0:0",
			}, {
				from:      0,
				through:   3600 * 1000, // ms
				tableName: "table",
				hashKey:   "0:1",
			}, {
				from:      0,
				through:   1800 * 1000, // ms
				tableName: "table",
				hashKey:   "0:2",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.hourlyBuckets(tt.args.from, tt.args.through, userID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDailyBuckets(t *testing.T) {
	const (
		userID     = "0"
		metricName = model.LabelValue("name")
		tableName  = "table"
	)
	var cfg = PeriodConfig{
		IndexTables: PeriodicTableConfig{Prefix: tableName},
	}

	type args struct {
		from    model.Time
		through model.Time
	}
	tests := []struct {
		name string
		args args
		want []Bucket
	}{
		{
			"0 day window",
			args{
				from:    model.TimeFromUnix(0),
				through: model.TimeFromUnix(0),
			},
			[]Bucket{{
				from:      0,
				through:   0,
				tableName: "table",
				hashKey:   "0:d0",
			}},
		},
		{
			"6 hour window",
			args{
				from:    model.TimeFromUnix(0),
				through: model.TimeFromUnix(6 * 3600),
			},
			[]Bucket{{
				from:      0,
				through:   (6 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d0",
			}},
		},
		{
			"1 day window",
			args{
				from:    model.TimeFromUnix(0),
				through: model.TimeFromUnix(24 * 3600),
			},
			[]Bucket{{
				from:      0,
				through:   (24 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d0",
			}, {
				from:      0,
				through:   0,
				tableName: "table",
				hashKey:   "0:d1",
			}},
		},
		{
			"window spanning 3 days with non-zero start",
			args{
				from:    model.TimeFromUnix(6 * 3600),
				through: model.TimeFromUnix((2 * 24 * 3600) + (12 * 3600)),
			},
			[]Bucket{{
				from:      (6 * 3600) * 1000,  // ms
				through:   (24 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d0",
			}, {
				from:      0,
				through:   (24 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d1",
			}, {
				from:      0,
				through:   (12 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d2",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.dailyBuckets(tt.args.from, tt.args.through, userID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestChunkTableFor(t *testing.T) {
	tablePeriod, err := time.ParseDuration("168h")
	require.NoError(t, err)

	periodConfigs := []PeriodConfig{
		{
			From: MustParseDayTime("1970-01-01"),
			IndexTables: PeriodicTableConfig{
				Prefix: "index_1_",
				Period: tablePeriod,
			},
			ChunkTables: PeriodicTableConfig{
				Prefix: "chunks_1_",
				Period: tablePeriod,
			},
		},
		{
			From: MustParseDayTime("2019-01-02"),
			IndexTables: PeriodicTableConfig{
				Prefix: "index_2_",
				Period: tablePeriod,
			},
			ChunkTables: PeriodicTableConfig{
				Prefix: "chunks_2_",
				Period: tablePeriod,
			},
		},
		{
			From: MustParseDayTime("2019-03-06"),
			IndexTables: PeriodicTableConfig{
				Prefix: "index_3_",
				Period: tablePeriod,
			},
			ChunkTables: PeriodicTableConfig{
				Prefix: "chunks_3_",
				Period: tablePeriod,
			},
		},
	}

	schemaCfg := SchemaConfig{
		Configs: periodConfigs,
	}

	testCases := []struct {
		timeStr    string // RFC3339
		chunkTable string
	}{
		{
			timeStr:    "1970-01-01T00:00:00Z",
			chunkTable: "chunks_1_0",
		},
		{
			timeStr:    "1970-01-01T00:00:01Z",
			chunkTable: "chunks_1_0",
		},
		{
			timeStr:    "2019-01-01T00:00:00Z",
			chunkTable: "chunks_1_2556",
		},
		{
			timeStr:    "2019-01-01T23:59:59Z",
			chunkTable: "chunks_1_2556",
		},
		{
			timeStr:    "2019-01-02T00:00:00Z",
			chunkTable: "chunks_2_2556",
		},
		{
			timeStr:    "2019-03-06T00:00:00Z",
			chunkTable: "chunks_3_2565",
		},
		{
			timeStr:    "2020-03-06T00:00:00Z",
			chunkTable: "chunks_3_2618",
		},
	}

	for _, tc := range testCases {
		ts, err := time.Parse(time.RFC3339, tc.timeStr)
		require.NoError(t, err)

		table, err := schemaCfg.ChunkTableFor(model.TimeFromUnix(ts.Unix()))
		require.NoError(t, err)

		require.Equal(t, tc.chunkTable, table)
	}
}

func TestSchemaConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config   *SchemaConfig
		expected error
	}{
		"should pass the default config (ie. used cortex runs with a target not requiring the schema config)": {
			config:   &SchemaConfig{},
			expected: nil,
		},
		"should fail on invalid schema version": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{Schema: "v0"},
				},
			},
			expected: errInvalidSchemaVersion,
		},
		"should fail on index table period not multiple of 1h for schema v1": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v1",
						IndexTables: PeriodicTableConfig{Period: 30 * time.Minute},
					},
				},
			},
			expected: errInvalidTablePeriod,
		},
		"should fail on chunk table period not multiple of 1h for schema v1": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v1",
						IndexTables: PeriodicTableConfig{Period: 6 * time.Hour},
						ChunkTables: PeriodicTableConfig{Period: 30 * time.Minute},
					},
				},
			},
			expected: errInvalidTablePeriod,
		},
		"should pass on index and chunk table period multiple of 1h for schema v1": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v1",
						IndexTables: PeriodicTableConfig{Period: 6 * time.Hour},
						ChunkTables: PeriodicTableConfig{Period: 6 * time.Hour},
					},
				},
			},
			expected: nil,
		},
		"should fail on index table period not multiple of 24h for schema v10": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v10",
						IndexTables: PeriodicTableConfig{Period: 6 * time.Hour},
					},
				},
			},
			expected: errInvalidTablePeriod,
		},
		"should fail on chunk table period not multiple of 24h for schema v10": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v10",
						IndexTables: PeriodicTableConfig{Period: 24 * time.Hour},
						ChunkTables: PeriodicTableConfig{Period: 6 * time.Hour},
					},
				},
			},
			expected: errInvalidTablePeriod,
		},
		"should pass on index and chunk table period multiple of 24h for schema v10": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v10",
						IndexTables: PeriodicTableConfig{Period: 24 * time.Hour},
						ChunkTables: PeriodicTableConfig{Period: 24 * time.Hour},
					},
				},
			},
			expected: nil,
		},
		"should pass on index and chunk table period set to zero (no period tables)": {
			config: &SchemaConfig{
				Configs: []PeriodConfig{
					{
						Schema:      "v10",
						IndexTables: PeriodicTableConfig{Period: 0},
						ChunkTables: PeriodicTableConfig{Period: 0},
					},
				},
			},
			expected: nil,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.config.Validate()
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func MustParseDayTime(s string) DayTime {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return DayTime{model.TimeFromUnix(t.Unix())}
}
