package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	week            int
	segments        int
	deleters        int
	deleteBatchSize int
	tableName       string
	address         string

	dynamoDB *dynamodb.DynamoDB

	// Readers send items on this chan to be deleted
	delete chan map[string]*dynamodb.AttributeValue
	retry  chan map[string]*dynamodb.AttributeValue
	// Deleters read batches of items from this chan
	batched chan []*dynamodb.WriteRequest
}

var (
	pagesPerDot int
)

func main() {
	var (
		schemaConfig  chunk.SchemaConfig
		storageConfig storage.Config

		orgsFile string

		scanner  scanner
		loglevel string
	)

	util.RegisterFlags(&storageConfig, &schemaConfig)
	flag.IntVar(&scanner.week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&scanner.segments, "segments", 1, "Number of segments to run in parallel")
	flag.IntVar(&scanner.deleters, "deleters", 1, "Number of deleters to run in parallel")
	flag.IntVar(&scanner.deleteBatchSize, "delete-batch-size", 25, "Number of delete requests to batch up")
	flag.StringVar(&scanner.address, "address", "localhost:6060", "Address to listen on, for profiling, etc.")
	flag.StringVar(&orgsFile, "delete-orgs-file", "", "File containing IDs of orgs to delete")
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

	orgs := map[int]struct{}{}
	if orgsFile != "" {
		content, err := ioutil.ReadFile(orgsFile)
		checkFatal(err)
		for _, arg := range strings.Fields(string(content)) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			orgs[org] = struct{}{}
		}
	}

	if scanner.week == 0 {
		scanner.week = int(time.Now().Unix() / int64(7*24*time.Hour/time.Second))
	}

	config, err := awscommon.ConfigFromURL(storageConfig.AWSStorageConfig.DynamoDB.URL)
	checkFatal(err)
	session := session.New(config)
	scanner.dynamoDB = dynamodb.New(session)

	scanner.tableName = fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, scanner.week)
	fmt.Printf("table %s\n", scanner.tableName)

	// Unbuffered chan so we can tell when batcher has received all items
	scanner.delete = make(chan map[string]*dynamodb.AttributeValue)
	scanner.retry = make(chan map[string]*dynamodb.AttributeValue, 100)
	scanner.batched = make(chan []*dynamodb.WriteRequest)

	var deleteGroup sync.WaitGroup
	deleteGroup.Add(1 + scanner.deleters)
	var pending sync.WaitGroup
	go func() {
		scanner.batcher(&pending)
		deleteGroup.Done()
	}()
	for i := 0; i < scanner.deleters; i++ {
		go func() {
			scanner.deleteLoop(&pending)
			deleteGroup.Done()
		}()
	}

	var readerGroup sync.WaitGroup
	readerGroup.Add(scanner.segments)
	totals := newSummary()
	var totalsMutex sync.Mutex

	for segment := 0; segment < scanner.segments; segment++ {
		go func(segment int) {
			handler := newHandler(orgs)
			handler.requests = scanner.delete
			err := scanner.segmentScan(segment, handler)
			checkFatal(err)
			totalsMutex.Lock()
			totals.accumulate(handler.summary)
			totalsMutex.Unlock()
			readerGroup.Done()
		}(segment)
	}
	// Wait until all reader segments have finished
	readerGroup.Wait()
	// Ensure that batcher has received all items so it won't call Add() any more
	scanner.delete <- nil
	// Wait for pending items to be sent to DynamoDB
	pending.Wait()
	// Close chans to signal deleter(s) and batcher to terminate
	close(scanner.batched)
	close(scanner.retry)
	deleteGroup.Wait()

	fmt.Printf("\n")
	totals.print()
}

func (sc *scanner) segmentScan(segment int, handler handler) error {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(sc.tableName),
		ProjectionExpression: aws.String(hashKey + "," + rangeKey),
		Segment:              aws.Int64(int64(segment)),
		TotalSegments:        aws.Int64(int64(sc.segments)),
		//ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	err := sc.dynamoDB.ScanPages(input, handler.handlePage)
	if err != nil {
		return err
	}
	return nil
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
	requests chan map[string]*dynamodb.AttributeValue
	summary
}

func newHandler(orgs map[int]struct{}) handler {
	return handler{
		orgs:    orgs,
		summary: newSummary(),
	}
}

func (h *handler) handlePage(page *dynamodb.ScanOutput, lastPage bool) bool {
	h.pages++
	if pagesPerDot > 0 && h.pages%pagesPerDot == 0 {
		fmt.Printf(".")
	}
	for _, m := range page.Items {
		org := orgFromHash(m[hashKey].S)
		if org <= 0 {
			continue
		}
		h.counts[org]++
		if _, found := h.orgs[org]; found {
			h.requests <- m // Send attributes to the chan
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

func throttled(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && (awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException)
}

func (sc *scanner) deleteLoop(pending *sync.WaitGroup) {
	for {
		batch, ok := <-sc.batched
		if !ok {
			return
		}
		level.Debug(util.Logger).Log("msg", "about to delete", "num_requests", len(batch))
		ret, err := sc.dynamoDB.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				sc.tableName: batch,
			},
		})
		if err != nil {
			if throttled(err) {
				// Send the whole request back into the batcher
				for _, item := range batch {
					sc.retry <- item.DeleteRequest.Key
				}
			} else {
				level.Error(util.Logger).Log("msg", "unable to delete", "err", err)
				pending.Add(-len(batch))
			}
			continue
		}
		count := 0
		// Send unprocessed items back into the batcher
		for _, items := range ret.UnprocessedItems {
			count += len(items)
			for _, item := range items {
				sc.retry <- item.DeleteRequest.Key
			}
		}
		pending.Add(-(len(batch) - count))
	}
}

// Receive individual requests, and batch them up into groups to send to DynamoDB
func (sc *scanner) batcher(pending *sync.WaitGroup) {
	finished := false
	var requests []*dynamodb.WriteRequest
	for {
		// We will allow in new data if the queue isn't too long
		var in chan map[string]*dynamodb.AttributeValue
		if len(requests) < 1000 {
			in = sc.delete
		}
		// We will send out a batch if the queue is big enough, or if we're finishing
		var out chan []*dynamodb.WriteRequest
		outlen := len(requests)
		if len(requests) >= sc.deleteBatchSize {
			out = sc.batched
			outlen = sc.deleteBatchSize
		} else if finished && len(requests) > 0 {
			out = sc.batched
		}
		var keyMap map[string]*dynamodb.AttributeValue
		var ok bool
		select {
		case keyMap = <-in:
			if keyMap == nil { // Nil used as interlock to know we received all previous values
				finished = true
			} else {
				pending.Add(1)
			}
		case keyMap, ok = <-sc.retry:
			if !ok {
				return
			}
		case out <- requests[:outlen]:
			requests = requests[outlen:]
		}
		if keyMap != nil {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: keyMap,
				},
			})
		}
	}
}
