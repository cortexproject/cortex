package chunk

import (
	"flag"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/mtime"
	"golang.org/x/net/context"
)

const (
	readLabel  = "read"
	writeLabel = "write"
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
	DynamoDB             DynamoDBClientValue
	DynamoDBPollInterval time.Duration

	PeriodicTableConfig

	// duration a table will be created before it is needed.
	CreationGracePeriod        time.Duration
	MaxChunkAge                time.Duration
	ProvisionedWriteThroughput int64
	ProvisionedReadThroughput  int64
	InactiveWriteThroughput    int64
	InactiveReadThroughput     int64
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *TableManagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.DynamoDB, "dynamodb.url", "DynamoDB endpoint URL.")
	f.DurationVar(&cfg.DynamoDBPollInterval, "dynamodb.poll-interval", 2*time.Minute, "How frequently to poll DynamoDB to learn our capacity.")
	f.DurationVar(&cfg.CreationGracePeriod, "dynamodb.periodic-table.grace-period", 10*time.Minute, "DynamoDB periodic tables grace period (duration which table will be created/deleted before/after it's needed).")
	f.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age time before flushing.")
	f.Int64Var(&cfg.ProvisionedWriteThroughput, "dynamodb.periodic-table.write-throughput", 3000, "DynamoDB periodic tables write throughput")
	f.Int64Var(&cfg.ProvisionedReadThroughput, "dynamodb.periodic-table.read-throughput", 300, "DynamoDB periodic tables read throughput")
	f.Int64Var(&cfg.InactiveWriteThroughput, "dynamodb.periodic-table.inactive-write-throughput", 1, "DynamoDB periodic tables write throughput for inactive tables.")
	f.Int64Var(&cfg.InactiveReadThroughput, "dynamodb.periodic-table.inactive-read-throughput", 300, "DynamoDB periodic tables read throughput for inactive tables")

	cfg.PeriodicTableConfig.RegisterFlags(f)
}

// DynamoTableManager creates and manages the provisioned throughput on DynamoDB tables
type DynamoTableManager struct {
	cfg  TableManagerConfig
	done chan struct{}
	wait sync.WaitGroup
}

// NewDynamoTableManager makes a new DynamoTableManager
func NewDynamoTableManager(cfg TableManagerConfig) (*DynamoTableManager, error) {
	m := &DynamoTableManager{
		cfg:  cfg,
		done: make(chan struct{}),
	}
	return m, nil
}

// Start the DynamoTableManager
func (m *DynamoTableManager) Start() {
	m.wait.Add(1)
	go m.loop()
}

// Stop the DynamoTableManager
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
	expected := m.calculateExpectedTables()
	log.Infof("Expecting %d tables", len(expected))

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

func (m *DynamoTableManager) calculateExpectedTables() []tableDescription {
	if !m.cfg.UsePeriodicTables {
		return []tableDescription{
			{
				name:             m.cfg.DynamoDB.TableName,
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
			name:             m.cfg.DynamoDB.TableName,
			provisionedRead:  m.cfg.InactiveReadThroughput,
			provisionedWrite: m.cfg.InactiveWriteThroughput,
		}

		// if we are before the switch to periodic table, we need to give this table write throughput
		if now < (firstTable*tablePeriodSecs)+gracePeriodSecs+maxChunkAgeSecs {
			legacyTable.provisionedRead = m.cfg.ProvisionedReadThroughput
			legacyTable.provisionedWrite = m.cfg.ProvisionedWriteThroughput
		}
		result = append(result, legacyTable)
	}

	for i := firstTable; i <= lastTable; i++ {
		table := tableDescription{
			// Name construction needs to be consistent with chunk_store.bigBuckets
			name:             m.cfg.TablePrefix + strconv.Itoa(int(i)),
			provisionedRead:  m.cfg.InactiveReadThroughput,
			provisionedWrite: m.cfg.InactiveWriteThroughput,
		}

		// if now is within table [start - grace, end + grace), then we need some write throughput
		if (i*tablePeriodSecs)-gracePeriodSecs <= now && now < (i*tablePeriodSecs)+tablePeriodSecs+gracePeriodSecs+maxChunkAgeSecs {
			table.provisionedRead = m.cfg.ProvisionedReadThroughput
			table.provisionedWrite = m.cfg.ProvisionedWriteThroughput
		}
		result = append(result, table)
	}

	sort.Sort(byName(result))
	return result
}

// partitionTables works out tables that need to be created vs tables that need to be updated
func (m *DynamoTableManager) partitionTables(ctx context.Context, descriptions []tableDescription) ([]tableDescription, []tableDescription, error) {
	existingTables, err := m.listTables(ctx)
	if err != nil {
		return nil, nil, err
	}

	toCreate, toCheckThroughput := []tableDescription{}, []tableDescription{}
	i, j := 0, 0
	for i < len(descriptions) && j < len(existingTables) {
		if descriptions[i].name < existingTables[j] {
			// Table descriptions[i] doesn't exist
			toCreate = append(toCreate, descriptions[i])
			i++
		} else if descriptions[i].name > existingTables[j] {
			// existingTables[j].name isn't in descriptions, can ignore
			j++
		} else {
			// Table exists, need to check it has correct throughput
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

func (m *DynamoTableManager) listTables(ctx context.Context) ([]string, error) {
	table := []string{}
	if err := instrument.TimeRequestHistogram(ctx, "DynamoDB.ListTablesPages", dynamoRequestDuration, func(_ context.Context) error {
		return m.cfg.DynamoDB.ListTablesPages(&dynamodb.ListTablesInput{}, func(resp *dynamodb.ListTablesOutput, _ bool) bool {
			for _, s := range resp.TableNames {
				table = append(table, *s)
			}
			return true
		})
	}); err != nil {
		return nil, err
	}
	sort.Strings(table)
	return table, nil
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
			_, err := m.cfg.DynamoDB.CreateTable(params)
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
			out, err = m.cfg.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{
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
			_, err := m.cfg.DynamoDB.UpdateTable(&dynamodb.UpdateTableInput{
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
