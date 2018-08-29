package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/go-kit/kit/log/level"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/logging"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

type scanner struct {
	week      int
	segments  int
	tableName string
	address   string

	dynamoDB *dynamodb.DynamoDB
}

var (
	pagesPerDot int
)

func main() {
	var (
		schemaConfig  chunk.SchemaConfig
		storageConfig storage.Config

		scanner  scanner
		loglevel string
	)

	util.RegisterFlags(&storageConfig, &schemaConfig)
	flag.IntVar(&scanner.week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&scanner.segments, "segments", 1, "Number of segments to run in parallel")
	flag.StringVar(&scanner.address, "address", "localhost:6060", "Address to listen on, for profiling, etc.")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.IntVar(&pagesPerDot, "pages-per-dot", 10, "Print a dot per N pages in DynamoDB (0 to disable)")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	// HTTP listener for profiling
	go func() {
		checkFatal(http.ListenAndServe(scanner.address, nil))
	}()

	if scanner.week == 0 {
		scanner.week = int(time.Now().Unix() / int64(7*24*time.Hour/time.Second))
	}

	config, err := awscommon.ConfigFromURL(storageConfig.AWSStorageConfig.DynamoDB.URL)
	checkFatal(err)
	session := session.New(config)
	scanner.dynamoDB = dynamodb.New(session)

	var group sync.WaitGroup
	group.Add(scanner.segments)
	totals := newSummary()
	var totalsMutex sync.Mutex

	scanner.tableName = fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, scanner.week)
	fmt.Printf("table %s\n", scanner.tableName)
	for segment := 0; segment < scanner.segments; segment++ {
		go func(segment int) {
			handler := newHandler()
			err := scanner.segmentScan(segment, handler)
			checkFatal(err)
			totalsMutex.Lock()
			totals.accumulate(handler.summary)
			totalsMutex.Unlock()
			group.Done()
		}(segment)
	}
	group.Wait()
	fmt.Printf("\n")
	totals.print()
}

func (sc scanner) segmentScan(segment int, handler handler) error {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(sc.tableName),
		ProjectionExpression: aws.String(hashKey),
		Segment:              aws.Int64(int64(segment)),
		TotalSegments:        aws.Int64(int64(sc.segments)),
		//ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	for _, arg := range flag.Args() {
		org, err := strconv.Atoi(arg)
		checkFatal(err)
		handler.orgs[org] = struct{}{}
	}

	err := sc.dynamoDB.ScanPages(input, handler.handlePage)
	if err != nil {
		return err
	}

	delete := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			sc.tableName: handler.requests,
		},
	}
	_ = delete
	//_, err = dynamoDB.BatchWriteItem(delete)
	return err
}

/* TODO: delete v8 schema rows for all instances */

const (
	hashKey  = "h"
	rangeKey = "r"
	valueKey = "c"
)

type summary struct {
	counts map[int]int
}

func newSummary() summary {
	return summary{
		counts: map[int]int{},
	}
}

func (s *summary) accumulate(b summary) {
	for k, v := range b.counts {
		s.counts[k] += v
	}
}

func (s summary) print() {
	for user, count := range s.counts {
		fmt.Printf("%d, %d\n", user, count)
	}
}

type handler struct {
	pages    int
	orgs     map[int]struct{}
	requests []*dynamodb.WriteRequest
	summary
}

func newHandler() handler {
	return handler{
		orgs:    map[int]struct{}{},
		summary: newSummary(),
	}
}

func (h *handler) reset() {
	h.requests = nil
	h.counts = map[int]int{}
}

func (h *handler) handlePage(page *dynamodb.ScanOutput, lastPage bool) bool {
	h.pages++
	if pagesPerDot > 0 && h.pages%pagesPerDot == 0 {
		fmt.Printf(".")
	}
	for _, m := range page.Items {
		hashVal := m[hashKey].S
		org := orgFromHash(hashVal)
		if org <= 0 { // unrecognized format
			continue
		}
		h.counts[org]++
		if _, found := h.orgs[org]; found {
			fmt.Printf("%s\n", *hashVal)
			h.requests = append(h.requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						hashKey: {S: hashVal},
					},
				},
			})
		}
	}
	return true
}

func orgFromHash(hashVal *string) int {
	hashStr := aws.StringValue(hashVal)
	if hashStr == "" {
		return -1
	}
	pos := strings.Index(hashStr, "/")
	if pos < 0 { // try index table format
		pos = strings.Index(hashStr, ":")
	}
	if pos < 0 { // unrecognized format
		return -1
	}
	org, err := strconv.Atoi(hashStr[:pos])
	if err != nil {
		return -1
	}
	return org
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		os.Exit(1)
	}
}
