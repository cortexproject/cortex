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
	tableCapacity *prometheus.GaugeVec

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
			log.Debugf("Updating limits from dynamo")
			err := w.updateTableLimits()
			if err != nil {
				log.Warnf("Could not fetch table limits from dynamo: %v", err)
			}
		case <-w.quit:
			ticker.Stop()
		}
	}
}

func (w *dynamoWatcher) updateTableLimits() error {
	output, err := w.dynamoDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &w.tableName,
	})
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
	w.tableCapacity.Describe(ch)
}

// Collect implements prometheus.Collector.
func (w *dynamoWatcher) Collect(ch chan<- prometheus.Metric) {
	w.tableCapacity.Collect(ch)
}
