package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
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

		deleteOrgsFile string
		includeOrgsStr string

		week      int64
		segments  int
		tableName string
		loglevel  string
		address   string

		rechunkTablePrefix string
		reindexTablePrefix string
	)

	flagext.RegisterFlags(&storageConfig, &schemaConfig, &chunkStoreConfig)
	flag.StringVar(&address, "address", ":6060", "Address to listen on, for profiling, etc.")
	flag.Int64Var(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
	flag.StringVar(&deleteOrgsFile, "delete-orgs-file", "", "File containing IDs of orgs to delete")
	flag.StringVar(&includeOrgsStr, "include-orgs", "", "IDs of orgs to include (space-separated)")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.StringVar(&rechunkTablePrefix, "dynamodb.rechunk-prefix", "", "Prefix of new chunk table (blank to disable)")
	flag.StringVar(&reindexTablePrefix, "dynamodb.reindex-prefix", "", "Prefix of new index table (blank to disable)")
	flag.BoolVar(&reEncodeChunks, "re-encode-chunks", false, "Enable re-encoding of chunks to save on storing zeros")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	// HTTP listener for profiling
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		checkFatal(http.ListenAndServe(address, nil))
	}()

	deleteOrgs, includeOrgs := setupOrgs(deleteOrgsFile, includeOrgsStr)

	secondsInWeek := int64(7 * 24 * time.Hour.Seconds())
	if week == 0 {
		week = time.Now().Unix() / secondsInWeek
	}

	schemaConfig.Load()

	overrides := &validation.Overrides{}
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	checkFatal(err)
	defer chunkStore.Stop()

	var reindexStore chunk.Store
	if reindexTablePrefix != "" || rechunkTablePrefix != "" {
		reindexSchemaConfig := schemaConfig
		//FIXME
		//reindexSchemaConfig.ChunkTables.Prefix = rechunkTablePrefix
		//reindexSchemaConfig.IndexTables.Prefix = reindexTablePrefix
		reindexStore, err = storage.NewStore(storageConfig, chunkStoreConfig, reindexSchemaConfig, overrides)
		checkFatal(err)
	}

	tableTime := model.TimeFromUnix(week * secondsInWeek)
	tableName, err = schemaConfig.ChunkTableFor(tableTime)
	checkFatal(err)
	fmt.Printf("table %s\n", tableName)

	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		handlers[segment] = newHandler(reindexStore, rechunkTablePrefix, reindexTablePrefix, includeOrgs, deleteOrgs)
		callbacks[segment] = handlers[segment].handlePage
	}

	err = chunkStore.(chunk.Store2).Scan(context.Background(), tableTime, tableTime, reindexTablePrefix != "", callbacks)
	checkFatal(err)

	if reindexStore != nil {
		reindexStore.Stop()
	}

	totals := newSummary()
	for segment := 0; segment < segments; segment++ {
		totals.accumulate(handlers[segment].summary)
	}
	totals.print(deleteOrgs)
}

func setupOrgs(deleteOrgsFile, includeOrgsStr string) (deleteOrgs, includeOrgs map[int]struct{}) {
	deleteOrgs = map[int]struct{}{}
	if deleteOrgsFile != "" {
		content, err := ioutil.ReadFile(deleteOrgsFile)
		checkFatal(err)
		for _, arg := range strings.Fields(string(content)) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			deleteOrgs[org] = struct{}{}
		}
	}

	includeOrgs = map[int]struct{}{}
	if includeOrgsStr != "" {
		for _, arg := range strings.Fields(includeOrgsStr) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			includeOrgs[org] = struct{}{}
		}
	}
	return
}

/* TODO: delete v8 schema rows for all instances */

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

func (s summary) print(deleteOrgs map[int]struct{}) {
	for instance, m := range s.counts {
		deleted := ""
		if _, found := deleteOrgs[instance]; found {
			deleted = "deleted"
		}
		for metric, c := range m {
			fmt.Printf("%d, %s, %d, %s\n", instance, metric, c, deleted)
		}
	}
}

type handler struct {
	store              chunk.Store
	tableName          string
	pages              int
	includeOrgs        map[int]struct{}
	deleteOrgs         map[int]struct{}
	reindexTablePrefix string
	summary
}

func newHandler(store chunk.Store, tableName string, reindexTablePrefix string, includeOrgs map[int]struct{}, deleteOrgs map[int]struct{}) handler {
	if len(includeOrgs) == 0 {
		includeOrgs = nil
	}
	return handler{
		store:              store,
		tableName:          tableName,
		includeOrgs:        includeOrgs,
		deleteOrgs:         deleteOrgs,
		summary:            newSummary(),
		reindexTablePrefix: reindexTablePrefix,
	}
}

// ReadBatchHashIterator is an iterator over a ReadBatch with a HashValue method.
type ReadBatchHashIterator interface {
	Next() bool
	RangeValue() []byte
	Value() []byte
	HashValue() string
}

func (h *handler) handlePage(page chunk.ReadBatch) {
	ctx := context.Background()
	pageCounter.WithLabelValues(h.tableName).Inc()
	decodeContext := chunk.NewDecodeContext()
	for i := page.Iterator().(ReadBatchHashIterator); i.Next(); {
		hashValue := i.HashValue()
		org := orgFromHash(hashValue)
		if org <= 0 {
			continue
		}
		if h.includeOrgs != nil {
			if _, found := h.includeOrgs[org]; !found {
				continue
			}
		}
		if h.counts[org] == nil {
			h.counts[org] = make(map[string]int)
		}
		h.counts[org][""]++
		if _, found := h.deleteOrgs[org]; found {
			//request := h.storageClient.NewWriteBatch()
			//request.AddDelete(h.tableName, hashValue, i.RangeValue())
			//			h.requests <- request
		} else if h.store != nil {
			var ch chunk.Chunk
			err := ch.Decode(decodeContext, i.Value())
			if err != nil {
				level.Error(util.Logger).Log("msg", "chunk decode error", "err", err)
				continue
			}
			h.counts[org][string(ch.Metric[model.MetricNameLabel])]++
			if h.tableName == "" { // just write index entries
				/*err = h.store.IndexChunk(ctx, ch) FIXME
				if err != nil {
					level.Error(util.Logger).Log("msg", "indexing error", "err", err)
					continue
				}*/
			} else {
				if reEncodeChunks {
					err = ch.Encode()
					if err != nil {
						level.Error(util.Logger).Log("msg", "re-encode error", "err", err)
						continue
					}
				}
				err = h.store.Put(ctx, []chunk.Chunk{ch})
				if err != nil {
					level.Error(util.Logger).Log("msg", "put error", "err", err)
					continue
				}
			}
		}
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
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		os.Exit(1)
	}
}
