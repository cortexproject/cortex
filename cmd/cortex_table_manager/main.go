package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"

	"github.com/weaveworks/cortex/chunk"
)

func main() {
	cfg := chunk.TableManagerConfig{
		PeriodicTableConfig: chunk.PeriodicTableConfig{
			UsePeriodicTables: true,
		},
	}

	addr := flag.String("web.listen-addr", ":80", "The address to listen on for HTTP requests.")
	flag.DurationVar(&cfg.DynamoDBPollInterval, "dynamodb.poll-interval", 2*time.Minute, "How frequently to poll DynamoDB to learn our capacity.")
	periodicTableStartAt := flag.String("dynamodb.periodic-table.start", "", "DynamoDB periodic tables start time.")
	dynamodbURL := flag.String("dynamodb.url", "localhost:8000", "DynamoDB endpoint URL.")
	flag.StringVar(&cfg.TablePrefix, "dynamodb.periodic-table.prefix", "cortex_", "DynamoDB table prefix for the periodic tables.")
	flag.DurationVar(&cfg.TablePeriod, "dynamodb.periodic-table.period", 7*24*time.Hour, "DynamoDB periodic tables period.")
	flag.DurationVar(&cfg.CreationGracePeriod, "dynamodb.periodic-table.grace-period", 10*time.Minute, "DynamoDB periodic tables grace period (duration which table will be created/delete before/after its needed).")
	flag.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age time before flushing.")
	flag.Int64Var(&cfg.ProvisionedWriteThroughput, "dynamodb.periodic-table.write-throughput", 3000, "DynamoDB periodic tables write throughput")
	flag.Int64Var(&cfg.ProvisionedReadThroughput, "dynamodb.periodic-table.read-throughput", 300, "DynamoDB periodic tables read throughput")
	flag.Parse()

	var err error
	cfg.PeriodicTableStartAt, err = time.Parse(time.RFC3339, *periodicTableStartAt)
	if err != nil {
		log.Fatalf("Error parsing dynamodb.periodic-table.start: %v", err)
	}

	cfg.DynamoDB, cfg.TableName, err = chunk.NewDynamoDBClient(*dynamodbURL)
	if err != nil {
		log.Fatalf("Error creating DynamoDB client: %v", err)
	}

	tableManager, err := chunk.NewDynamoTableManager(cfg)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB table manager: %v", err)
	}
	tableManager.Start()
	defer tableManager.Stop()

	http.Handle("/metrics", prometheus.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
