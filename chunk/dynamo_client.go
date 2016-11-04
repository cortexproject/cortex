package chunk

import (
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	readLabel  = "read"
	writeLabel = "write"
)

type dynamoWatcher struct {
	accountMaxCapacity *prometheus.GaugeVec
	tableCapacity      *prometheus.GaugeVec

	dynamoDB  *dynamodb.DynamoDB
	tableName string

	updateInterval time.Duration
	quit           chan struct{}
	wait           sync.WaitGroup
}

// Watcher watches something and reports to Prometheus.
type Watcher interface {
	Stop()
	prometheus.Collector
}

// WatchDynamo watches Dynamo and reports on resource limits.
func WatchDynamo(dynamoDBURL string, interval time.Duration) (Watcher, error) {
	url, err := url.Parse(dynamoDBURL)
	if err != nil {
		return nil, err
	}
	dynamoDBConfig, err := awsConfigFromURL(url)
	if err != nil {
		return nil, err
	}
	client := dynamodb.New(session.New(dynamoDBConfig))

	tableName := strings.TrimPrefix(url.Path, "/")
	w := &dynamoWatcher{
		accountMaxCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "dynamo_account_max_capacity_units",
			Help:      "Account-wide DynamoDB capacity, measured in DynamoDB capacity units.",
		}, []string{"op"}),
		tableCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "dynamo_table_capacity_units",
			Help:      "Per-table DynamoDB capacity, measured in DynamoDB capacity units.",
		}, []string{"op", "table"}),
		dynamoDB:       client,
		tableName:      tableName,
		updateInterval: interval,
		quit:           make(chan struct{}),
	}
	go w.updateLoop()
	return w, nil
}

// Stop stops the dynamo watcher.
func (w *dynamoWatcher) Stop() {
	close(w.quit)
	w.wait.Wait()
}

func (w *dynamoWatcher) updateLoop() {
	defer w.wait.Done()
	ticker := time.NewTicker(w.updateInterval)
	for {
		select {
		case <-ticker.C:
			err := w.updateAccountLimits()
			if err != nil {
				// TODO: Back off if err is throttling related.
				log.Warnf("Could not fetch limits from dynamo: %v", err)
			}
		case <-w.quit:
			ticker.Stop()
		}
	}
}

func (w *dynamoWatcher) updateAccountLimits() error {
	limits, err := w.dynamoDB.DescribeLimits(&dynamodb.DescribeLimitsInput{})
	if err != nil {
		return err
	}
	w.accountMaxCapacity.WithLabelValues(readLabel).Set(float64(*limits.AccountMaxReadCapacityUnits))
	w.accountMaxCapacity.WithLabelValues(writeLabel).Set(float64(*limits.AccountMaxWriteCapacityUnits))
	return nil
}

func (w *dynamoWatcher) updateTableLimits() error {
	output, err := w.dynamoDB.DescribeTable(&dynamodb.DescribeTableInput{})
	if err != nil {
		return err
	}
	throughput := output.Table.ProvisionedThroughput
	w.tableCapacity.WithLabelValues(readLabel, w.tableName).Set(float64(*throughput.ReadCapacityUnits))
	w.tableCapacity.WithLabelValues(writeLabel, w.tableName).Set(float64(*throughput.WriteCapacityUnits))
	return nil
}

// Describe implements prometheus.Collector.
func (w *dynamoWatcher) Describe(ch chan<- *prometheus.Desc) {
	w.accountMaxCapacity.Describe(ch)
	w.tableCapacity.Describe(ch)
}

// Collect implements prometheus.Collector.
func (w *dynamoWatcher) Collect(ch chan<- prometheus.Metric) {
	w.accountMaxCapacity.Collect(ch)
	w.tableCapacity.Collect(ch)
}
