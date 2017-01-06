package chunk

import (
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/weaveworks/common/mtime"
	"golang.org/x/net/context"
)

const (
	tablePrefix = "cortex_"
	tablePeriod = 7 * 24 * time.Hour
	gracePeriod = 15 * time.Minute
	maxChunkAge = 12 * time.Hour
	write       = 200
	read        = 100
)

func TestDynamoTableManager(t *testing.T) {
	dynamodb := NewMockDynamoDB(0, 0)

	cfg := TableManagerConfig{
		dynamodb: dynamodb,

		PeriodicTableConfig: PeriodicTableConfig{
			UsePeriodicTables:    true,
			TablePrefix:          tablePrefix,
			TablePeriod:          tablePeriod,
			PeriodicTableStartAt: time.Unix(0, 0),
		},

		CreationGracePeriod:        gracePeriod,
		MaxChunkAge:                maxChunkAge,
		ProvisionedWriteThroughput: write,
		ProvisionedReadThroughput:  read,
	}
	tableManager, err := NewDynamoTableManager(cfg)
	if err != nil {
		t.Fatal(err)
	}

	test := func(name string, tm time.Time, expected []tableDescription) {
		t.Run(name, func(t *testing.T) {
			mtime.NowForce(tm)
			if err := tableManager.syncTables(context.Background()); err != nil {
				t.Fatal(err)
			}
			expectTables(t, dynamodb, expected)
		})
	}

	// Check at time zero, we have the base table and one weekly table
	test(
		"Initial test",
		time.Unix(0, 0),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: write},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Check running twice doesn't change anything
	test(
		"Nothing changed",
		time.Unix(0, 0),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: write},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Fast forward grace period, check we still have write throughput on base table
	test(
		"Move forward by grace period",
		time.Unix(0, 0).Add(gracePeriod),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: write},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Fast forward max chunk age + grace period, check write throughput on base table has gone
	test(
		"Move forward by max chunk age + grace period",
		time.Unix(0, 0).Add(maxChunkAge).Add(gracePeriod),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Fast forward table period - grace period, check we add another weekly table
	test(
		"Move forward by table period - grace period",
		time.Unix(0, 0).Add(tablePeriod).Add(-gracePeriod),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: write},
			{name: tablePrefix + "1", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Fast forward table period + grace period, check we still have provisioned throughput
	test(
		"Move forward by table period + grace period",
		time.Unix(0, 0).Add(tablePeriod).Add(gracePeriod),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: write},
			{name: tablePrefix + "1", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Fast forward table period + max chunk age + grace period, check we remove provisioned throughput
	test(
		"Move forward by table period + max chunk age + grace period",
		time.Unix(0, 0).Add(tablePeriod).Add(maxChunkAge).Add(gracePeriod),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "1", provisionedRead: read, provisionedWrite: write},
		},
	)

	// Check running twice doesn't change anything
	test(
		"Nothing changed",
		time.Unix(0, 0).Add(tablePeriod).Add(maxChunkAge).Add(gracePeriod),
		[]tableDescription{
			{name: "", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "0", provisionedRead: read, provisionedWrite: minWriteCapacity},
			{name: tablePrefix + "1", provisionedRead: read, provisionedWrite: write},
		},
	)
}

func expectTables(t *testing.T, dynamo dynamodbClient, expected []tableDescription) {
	tables := []string{}
	if err := dynamo.ListTablesPages(&dynamodb.ListTablesInput{}, func(resp *dynamodb.ListTablesOutput, _ bool) bool {
		for _, s := range resp.TableNames {
			tables = append(tables, *s)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}

	if len(expected) != len(tables) {
		t.Fatalf("Unexpected number of tables: %v != %v", expected, tables)
	}

	sort.Strings(tables)
	sort.Sort(byName(expected))

	for i, desc := range expected {
		if tables[i] != desc.name {
			t.Fatalf("Expected '%s', found '%s'", desc.name, tables[i])
		}

		out, err := dynamo.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(desc.name),
		})
		if err != nil {
			t.Fatal(err)
		}

		if *out.Table.ProvisionedThroughput.ReadCapacityUnits != desc.provisionedRead {
			t.Fatalf("Expected '%d', found '%d' for table '%s'", desc.provisionedRead, *out.Table.ProvisionedThroughput.ReadCapacityUnits, desc.name)
		}

		if *out.Table.ProvisionedThroughput.WriteCapacityUnits != desc.provisionedWrite {
			t.Fatalf("Expected '%d', found '%d' for table '%s'", desc.provisionedWrite, *out.Table.ProvisionedThroughput.WriteCapacityUnits, desc.name)
		}
	}
}
