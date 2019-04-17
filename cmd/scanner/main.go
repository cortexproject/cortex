package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	pageCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "pages_scanned_total",
		Help:      "Total count of pages scanned from a table",
	}, []string{"table"})

	reEncodeChunks bool
)

func main() {
	var (
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		chunkStoreConfig chunk.StoreConfig

		week      int64
		segments  int
		tableName string
		loglevel  string
		address   string
	)

	flagext.RegisterFlags(&storageConfig, &schemaConfig, &chunkStoreConfig)
	flag.StringVar(&address, "address", ":6060", "Address to listen on, for profiling, etc.")
	flag.Int64Var(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	// HTTP listener for profiling
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		checkFatal(http.ListenAndServe(address, nil))
	}()

	secondsInWeek := int64(7 * 24 * time.Hour.Seconds())
	if week == 0 {
		week = time.Now().Unix() / secondsInWeek
	}

	overrides := &validation.Overrides{}
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	checkFatal(err)
	defer chunkStore.Stop()

	tableName = "FIXME" //fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, week)
	fmt.Printf("table %s\n", tableName)

	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		handlers[segment] = newHandler(tableName)
		callbacks[segment] = handlers[segment].handlePage
	}

	tableTime := model.TimeFromUnix(week * secondsInWeek)
	err = chunkStore.(chunk.Store2).Scan(context.Background(), tableTime, tableTime, true, callbacks)
	checkFatal(err)

	totals := newSummary()
	for segment := 0; segment < segments; segment++ {
		totals.accumulate(handlers[segment].summary)
	}
	totals.print()
}

type summary struct {
	// Map from instance to (metric->count)
	counts map[int]map[string]int
}

func newSummary() summary {
	return summary{
		counts: map[int]map[string]int{},
	}
}

func (s *summary) accumulate(b summary) {
	for instance, m := range b.counts {
		if s.counts[instance] == nil {
			s.counts[instance] = make(map[string]int)
		}
		for metric, c := range m {
			s.counts[instance][metric] += c
		}
	}
}

func (s summary) print() {
	for instance, m := range s.counts {
		for metric, c := range m {
			fmt.Printf("%d, %s, %d\n", instance, metric, c)
		}
	}
}

type handler struct {
	tableName string
	summary
}

func newHandler(tableName string) handler {
	return handler{
		tableName: tableName,
		summary:   newSummary(),
	}
}

// ReadBatchHashIterator is an iterator over a ReadBatch with a HashValue method.
type ReadBatchHashIterator interface {
	Next() bool
	RangeValue() []byte
	Value() []byte
	HashValue() []byte
}

func (h *handler) handlePage(page chunk.ReadBatch) {
	pageCounter.WithLabelValues(h.tableName).Inc()
	decodeContext := chunk.NewDecodeContext()
	for i := page.Iterator().(ReadBatchHashIterator); i.Next(); {
		hashValue := i.HashValue()
		org := orgFromHash(string(hashValue))
		if h.counts[org] == nil {
			h.counts[org] = make(map[string]int)
		}
		h.counts[org][""]++
		var ch chunk.Chunk
		err := ch.Decode(decodeContext, i.Value())
		if err != nil {
			level.Error(util.Logger).Log("msg", "chunk decode error", "err", fmt.Sprintf("%+v", err))
			continue
		}
		h.counts[org][string(ch.Metric[model.MetricNameLabel])]++
	}
}

func orgFromHash(hashStr string) int {
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
		level.Error(util.Logger).Log("msg", "fatal error", "err", fmt.Sprintf("%v", err))
		os.Exit(1)
	}
}
