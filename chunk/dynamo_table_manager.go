package chunk

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/weaveworks/common/mtime"
	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"
)

const (
	readLabel        = "read"
	writeLabel       = "write"
	minWriteCapacity = 1
)

var (
	syncTableDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamo_sync_tables_seconds",
		Help:      "Time spent doing syncTables.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"})
	tableCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "dynamo_table_capacity_units",
		Help:      "Per-table DynamoDB capacity, measured in DynamoDB capacity units.",
	}, []string{"op", "table"})
)

func init() {
	prometheus.MustRegister(tableCapacity)
}

// TableManagerConfig is the config for a DynamoTableManager
type TableManagerConfig struct {
	DynamoDBURL          string
	DynamoDBPollInterval time.Duration

	UsePeriodicTables    bool
	TablePrefix          string
	TablePeriod          time.Duration
	PeriodicTableStartAt time.Time

	// duration a table will be created before it is needed.
	CreationGracePeriod        time.Duration
	MaxChunkAge                time.Duration
	ProvisionedWriteThroughput int64
	ProvisionedReadThroughput  int64

	// Not exported as only used by tests to inject mocks
	dynamodb dynamodbClient
}

// DynamoTableManager creates and manages the provisioned throughput on DynamoDB tables
type DynamoTableManager struct {
	cfg      TableManagerConfig
	dynamodb dynamodbClient

	// the name of the single, pre-weekly table
	tableName string

	done chan struct{}
	wait sync.WaitGroup
}

// NewDynamoTableManager makes a new DynamoTableManager
func NewDynamoTableManager(cfg TableManagerConfig) (*DynamoTableManager, error) {
	dynamodbClient, tableName := cfg.dynamodb, ""
	if dynamodbClient == nil {
		dynamodbURL, err := url.Parse(cfg.DynamoDBURL)
		if err != nil {
			return nil, err
		}

		dynamoDBConfig, err := awsConfigFromURL(dynamodbURL)
		if err != nil {
			return nil, err
		}

		dynamodbClient = dynamoClientAdapter{dynamodb.New(session.New(dynamoDBConfig))}
		tableName = strings.TrimPrefix(dynamodbURL.Path, "/")
	}

	m := &DynamoTableManager{
		dynamodb:  dynamodbClient,
		tableName: tableName,

		cfg:  cfg,
		done: make(chan struct{}),
	}
	return m, nil
}

// Start starts the DynamoTableManager
func (m *DynamoTableManager) Start() {
	m.wait.Add(1)
	go m.loop()
}

// Stop stops the DynamoTableManager
func (m *DynamoTableManager) Stop() {
	close(m.done)
	m.wait.Wait()
}

func (m *DynamoTableManager) loop() {
	defer m.wait.Done()

	ticker := time.NewTicker(m.cfg.DynamoDBPollInterval)
	defer ticker.Stop()

	if err := instrument.TimeRequestHistogram(context.Background(), "DynamoTableManager.syncTables", syncTableDuration, func(ctx context.Context) error {
		return m.syncTables(ctx)
	}); err != nil {
		log.Errorf("Error syncing tables: %v", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := instrument.TimeRequestHistogram(context.Background(), "DynamoTableManager.syncTables", syncTableDuration, func(ctx context.Context) error {
				return m.syncTables(ctx)
			}); err != nil {
				log.Errorf("Error syncing tables: %v", err)
			}
		case <-m.done:
			return
		}
	}
}

func (m *DynamoTableManager) syncTables(ctx context.Context) error {
	expected := m.calculateExpectedTables(ctx)

	toCreate, toCheckThroughput, err := m.partitionTables(ctx, expected)
	if err != nil {
		return err
	}

	if err := m.createTables(ctx, toCreate); err != nil {
		return err
	}

	return m.updateTables(ctx, toCheckThroughput)
}

type tableDescription struct {
	name             string
	provisionedRead  int64
	provisionedWrite int64
}

type byName []tableDescription

func (a byName) Len() int           { return len(a) }
func (a byName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool { return a[i].name < a[j].name }

func (m *DynamoTableManager) calculateExpectedTables(_ context.Context) []tableDescription {
	if !m.cfg.UsePeriodicTables {
		return []tableDescription{
			{
				name:             m.tableName,
				provisionedRead:  m.cfg.ProvisionedReadThroughput,
				provisionedWrite: m.cfg.ProvisionedWriteThroughput,
			},
		}
	}

	result := []tableDescription{}

	var (
		tablePeriodSecs = int64(m.cfg.TablePeriod / time.Second)
		gracePeriodSecs = int64(m.cfg.CreationGracePeriod / time.Second)
		maxChunkAgeSecs = int64(m.cfg.MaxChunkAge / time.Second)
		firstTable      = m.cfg.PeriodicTableStartAt.Unix() / tablePeriodSecs
		lastTable       = (mtime.Now().Unix() + gracePeriodSecs) / tablePeriodSecs
		now             = mtime.Now().Unix()
	)

	// Add the legacy table
	{
		legacyTable := tableDescription{
			name:             m.tableName,
			provisionedRead:  m.cfg.ProvisionedReadThroughput,
			provisionedWrite: minWriteCapacity,
		}

		// if we are before the switch to periodic table, we need to give this table write throughput
		if now < (firstTable*tablePeriodSecs)+gracePeriodSecs+maxChunkAgeSecs {
			legacyTable.provisionedWrite = m.cfg.ProvisionedWriteThroughput
		}
		result = append(result, legacyTable)
	}

	for i := firstTable; i <= lastTable; i++ {
		table := tableDescription{
			// Name construction needs to be consistent with chunk_store.bigBuckets
			name:             m.cfg.TablePrefix + strconv.Itoa(int(i)),
			provisionedRead:  m.cfg.ProvisionedReadThroughput,
			provisionedWrite: minWriteCapacity,
		}

		// if now is within table [start - grace, end + grace), then we need some write throughput
		if (i*tablePeriodSecs)-gracePeriodSecs <= now && now < (i*tablePeriodSecs)+tablePeriodSecs+gracePeriodSecs+maxChunkAgeSecs {
			table.provisionedWrite = m.cfg.ProvisionedWriteThroughput
		}
		result = append(result, table)
	}

	sort.Sort(byName(result))
	return result
}

func (m *DynamoTableManager) partitionTables(ctx context.Context, descriptions []tableDescription) ([]tableDescription, []tableDescription, error) {
	// Get a list of tables that exist
	existingTables := []string{}
	if err := instrument.TimeRequestHistogram(ctx, "DynamoDB.ListTablesPages", dynamoRequestDuration, func(_ context.Context) error {
		return m.dynamodb.ListTablesPages(&dynamodb.ListTablesInput{}, func(resp *dynamodb.ListTablesOutput, _ bool) bool {
			for _, s := range resp.TableNames {
				existingTables = append(existingTables, *s)
			}
			return true
		})
	}); err != nil {
		return nil, nil, err
	}
	sort.Strings(existingTables)

	// Work out tables that need to be created vs tables that need to be updated
	toCreate, toCheckThroughput := []tableDescription{}, []tableDescription{}
	i, j := 0, 0
	for i < len(descriptions) && j < len(existingTables) {
		if descriptions[i].name < existingTables[j] {
			// Table descriptions[i] doesn't exist
			toCreate = append(toCreate, descriptions[i])
		} else if descriptions[i].name > existingTables[j] {
			// existingTables[j].name isn't in descriptions, can ignore
			j++
		} else {
			// Table existis, need to check it has correct throughput
			toCheckThroughput = append(toCheckThroughput, descriptions[i])
			i++
			j++
		}
	}
	for ; i < len(descriptions); i++ {
		toCreate = append(toCreate, descriptions[i])
	}

	return toCreate, toCheckThroughput, nil
}

func (m *DynamoTableManager) createTables(ctx context.Context, descriptions []tableDescription) error {
	for _, desc := range descriptions {
		params := &dynamodb.CreateTableInput{
			TableName: aws.String(desc.name),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(hashKey),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String(rangeKey),
					AttributeType: aws.String("B"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(hashKey),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String(rangeKey),
					KeyType:       aws.String("RANGE"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(desc.provisionedRead),
				WriteCapacityUnits: aws.Int64(desc.provisionedWrite),
			},
		}
		log.Infof("Creating table %s", desc.name)
		if err := instrument.TimeRequestHistogram(ctx, "DynamoDB.CreateTable", dynamoRequestDuration, func(_ context.Context) error {
			_, err := m.dynamodb.CreateTable(params)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *DynamoTableManager) updateTables(ctx context.Context, descriptions []tableDescription) error {
	for _, desc := range descriptions {
		log.Infof("Checking provisioned throughput on table %s", desc.name)
		var out *dynamodb.DescribeTableOutput
		if err := instrument.TimeRequestHistogram(ctx, "DynamoDB.DescribeTable", dynamoRequestDuration, func(_ context.Context) error {
			var err error
			out, err = m.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
				TableName: aws.String(desc.name),
			})
			return err
		}); err != nil {
			return err
		}

		if *out.Table.TableStatus != dynamodb.TableStatusActive {
			log.Infof("Skipping update on  table %s, not yet ACTIVE", desc.name)
			continue
		}

		tableCapacity.WithLabelValues(readLabel, desc.name).Set(float64(*out.Table.ProvisionedThroughput.ReadCapacityUnits))
		tableCapacity.WithLabelValues(writeLabel, desc.name).Set(float64(*out.Table.ProvisionedThroughput.WriteCapacityUnits))

		if *out.Table.ProvisionedThroughput.ReadCapacityUnits == desc.provisionedRead && *out.Table.ProvisionedThroughput.WriteCapacityUnits == desc.provisionedWrite {
			log.Infof("  Provisioned throughput: read = %d, write = %d, skipping.", *out.Table.ProvisionedThroughput.ReadCapacityUnits, *out.Table.ProvisionedThroughput.WriteCapacityUnits)
			continue
		}

		log.Infof("  Updating provisioned throughput on table %s to read = %d, write = %d", desc.name, desc.provisionedRead, desc.provisionedWrite)
		if err := instrument.TimeRequestHistogram(ctx, "DynamoDB.DescribeTable", dynamoRequestDuration, func(_ context.Context) error {
			_, err := m.dynamodb.UpdateTable(&dynamodb.UpdateTableInput{
				TableName: aws.String(desc.name),
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(desc.provisionedRead),
					WriteCapacityUnits: aws.Int64(desc.provisionedWrite),
				},
			})
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}
