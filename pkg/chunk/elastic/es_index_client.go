package elastic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"

	"github.com/go-kit/kit/log/level"
	"github.com/olivere/elastic"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
)

const (
	null = string('\xff')
)

var esRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name: "es_request_duration_seconds",
	Help: "Time spent doing ElasticSearch requests.",

	Buckets: []float64{.025, .05, .1, .25, .5, 1, 2, 4, 8, 16, 32},
}, []string{"operation", "status_code"}))

func init() {
	esRequestDuration.Register()
}

// Config for a ElasticSearch index client.
type Config struct {
	Address       string `yaml:"address"`
	TemplateName  string `yaml:"template_name"`
	IndexType     string `yaml:"index_type"`
	MaxFetchDocs  int    `yaml:"max_fetch_docs"`
	NumOfShards   int    `yaml:"number_of_shards"`
	NumOfReplicas int    `yaml:"number_of_replicas"`
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
	TLSSkipVerify bool   `yaml:"tls_skip_verify"`
	CertFile      string `yaml:"cert_file"`
	KeyFile       string `yaml:"key_file"`
	CaFile        string `yaml:"ca_file"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "experimental.elastic.address", "http://127.0.0.1:9200", "Address of ElasticSearch.")
	f.StringVar(&cfg.TemplateName, "experimental.elastic.template-name", "cortextemplate", "Template name in ElasticSearch.")
	f.StringVar(&cfg.IndexType, "experimental.elastic.index-type", "cortexindex", "Index Type used in ElasticSearch.")
	f.IntVar(&cfg.MaxFetchDocs, "experimental.elastic.max-fetch-docs", 1000, "Max Fetch Docs for one page.")
	f.IntVar(&cfg.NumOfShards, "experimental.elastic.number-of-shards", 3, "Number of shards in creating template.")
	f.IntVar(&cfg.NumOfReplicas, "experimental.elastic.number-of-replicas", 1, "Number of replicas in creating template.")
	f.StringVar(&cfg.Username, "experimental.elastic.username", "", "User used in ElasticSearch Basic Auth.")
	f.StringVar(&cfg.Password, "experimental.elastic.password", "", "Password used in ElasticSearch Basic Auth.")
	f.BoolVar(&cfg.TLSSkipVerify, "experimental.elastic.tls-skip-verify", false, "Skip tls verify or not. Default is not to skip.")
	f.StringVar(&cfg.CertFile, "experimental.elastic.cert-file", "", "Cert File Location used in TLS Verify.")
	f.StringVar(&cfg.KeyFile, "experimental.elastic.key-file", "", "Key File Location used in TLS Verify.")
	f.StringVar(&cfg.CaFile, "experimental.elastic.ca-file", "", "CA File Location used in TLS Verify.")
}

// IndexEntry describes an entry in the chunk index
type IndexEntry struct {
	Hash  string `json:"hash"`
	Range string `json:"range"`
	Value string `json:"value,omitempty"`
}

var client *elastic.Client

type esClient struct {
	cfg    Config
	client *elastic.Client
}

func (e *esClient) Stop() {
	e.client.Stop()
}

type writeBatch struct {
	entries []chunk.IndexEntry
}

func (b *writeBatch) Delete(tableName, hashValue string, rangeValue []byte) {
	panic("ElasticSearch does not support Deleting index entries yet")
}

func (e *esClient) NewWriteBatch() chunk.WriteBatch {
	return &writeBatch{}
}

func (b *writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.entries = append(b.entries, chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

func (e *esClient) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	b := batch.(*writeBatch)

	indexName := b.entries[0].TableName

	bulkRequest := e.client.Bulk()
	for _, entry := range b.entries {
		index := IndexEntry{Hash: entry.HashValue, Range: string(entry.RangeValue), Value: string(entry.Value)}
		req := elastic.NewBulkIndexRequest().Index(indexName).Type(e.cfg.IndexType).Doc(index)
		bulkRequest = bulkRequest.Add(req)
	}

	err := instrument.CollectedRequest(ctx, "ES.BatchWrite", esRequestDuration,
		instrument.ErrorCode, func(ctx context.Context) error {
			var err error
			_, err = bulkRequest.Do(ctx)
			return err
		})

	if err != nil {
		return err
	}

	return nil
}

// readBatch represents a batch of rows read from ElasticSearch.
type readBatch struct {
	rangeValue []byte
	value      []byte
}

func (r readBatch) Iterator() chunk.ReadBatchIterator {
	return &elasticReadBatchIterator{
		readBatch: r,
	}
}

type elasticReadBatchIterator struct {
	consumed bool
	readBatch
}

func (r *elasticReadBatchIterator) Next() bool {
	if r.consumed {
		return false
	}
	r.consumed = true
	return true
}

func (r *elasticReadBatchIterator) RangeValue() []byte {
	return r.rangeValue
}

func (r *elasticReadBatchIterator) Value() []byte {
	return r.value
}

func (e *esClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return chunk_util.DoParallelQueries(ctx, e.query, queries, callback)
}

func (e *esClient) query(ctx context.Context, query chunk.IndexQuery, callback func(chunk.ReadBatch) (shouldContinue bool)) error {
	var rangeQuery *elastic.RangeQuery
	var valueTermQuery *elastic.TermQuery

	level.Debug(util.Logger).Log("msg", fmt.Sprintf(
		"hash [%s], rangeValuePrefix [%s], rangeValueStart [%s]", query.HashValue, query.RangeValuePrefix, query.RangeValueStart))
	hashTermQuery := elastic.NewTermQuery("hash", query.HashValue)
	switch {
	//case 1 - only has range query with rangePrefix.
	case len(query.RangeValuePrefix) > 0 && query.ValueEqual == nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValuePrefix)).
			Lt(string(query.RangeValuePrefix) + null)

	//case 2 - both has range query with rangePrefix, and value query.
	case len(query.RangeValuePrefix) > 0 && query.ValueEqual != nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValuePrefix)).
			Lt(string(query.RangeValuePrefix) + null)
		valueTermQuery = elastic.NewTermQuery("value", query.ValueEqual)

	//case 3 - only has range query with rangeStart
	case len(query.RangeValueStart) > 0 && query.ValueEqual == nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValueStart))

	//case 4 - both has range query with rangeStart, and value query
	case len(query.RangeValueStart) > 0 && query.ValueEqual != nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValueStart))
		valueTermQuery = elastic.NewTermQuery("value", query.ValueEqual)

	//case 5 - only has value query
	case query.ValueEqual != nil:
		valueTermQuery = elastic.NewTermQuery("value", query.ValueEqual)

	//case 6 - value is nil
	case query.ValueEqual == nil:
		break
	}

	// Build the query
	baseQuery := e.client.Search().Index(query.TableName).Size(e.cfg.MaxFetchDocs).
		Query(hashTermQuery).Sort("_id", true)
	if valueTermQuery != nil {
		baseQuery = baseQuery.Query(valueTermQuery)
	}
	if rangeQuery != nil {
		baseQuery = baseQuery.Query(rangeQuery)
	}

	err := instrument.CollectedRequest(ctx, "ES.Query", esRequestDuration,
		instrument.ErrorCode, func(ctx context.Context) error {
			// init first query
			searchResult, err := baseQuery.Do(ctx) // execute
			if err != nil {
				level.Error(util.Logger).Log("msg", fmt.Sprintf("Query in index %s met error!", query.TableName))
				return nil
			}
			var batch readBatch
			var ttyp IndexEntry
			for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
				if t, ok := item.(IndexEntry); ok {
					level.Debug(util.Logger).Log("msg", fmt.Sprintf("Index by hash %s: range %s, value %s\n", t.Hash, t.Range, t.Value))
					batch.rangeValue = []byte(t.Range)
					batch.value = []byte(t.Value)

					if !callback(&batch) {
						return nil
					}
				}
			}
			if searchResult.Hits.TotalHits > int64(e.cfg.MaxFetchDocs) { // need to run "search_after"
				var sortValue interface{}
				for {
					if len(searchResult.Hits.Hits) == 0 {
						break
					}
					//  Get Sort value from last SearchResult
					sortValue = searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort[0]

					// Build the query
					// build base Query again due to limitation of ElasticSearch Client
					// https://github.com/olivere/elastic/issues/1260
					// hopefully we can get an elegant way (maybe like baseQuery.SearchAfterClear()) to do this in the future.
					baseQuery = e.client.Search().Index(query.TableName).Size(e.cfg.MaxFetchDocs).
						Query(hashTermQuery).Sort("_id", true)
					if valueTermQuery != nil {
						baseQuery = baseQuery.Query(valueTermQuery)
					}
					if rangeQuery != nil {
						baseQuery = baseQuery.Query(rangeQuery)
					}
					// Search
					searchResult, err = baseQuery.SearchAfter(sortValue).Do(ctx) // execute
					if err == io.EOF {
						break
					}

					if err != nil {
						level.Error(util.Logger).Log("msg", fmt.Sprintf("Query in index %s met error!", query.TableName))
						return nil
					}
					var batch readBatch
					var ttyp IndexEntry
					for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
						if t, ok := item.(IndexEntry); ok {
							level.Debug(util.Logger).Log("msg", fmt.Sprintf("Index by hash %s: range %s, value %s\n", t.Hash, t.Range, t.Value))
							batch.rangeValue = []byte(t.Range)
							batch.value = []byte(t.Value)

							if !callback(&batch) {
								return nil
							}
						}
					}
				}
			}
			return nil
		})

	if err != nil {
		return err
	}

	return nil
}

// NewESIndexClient creates a new IndexClient that used ElasticSearch.
func NewESIndexClient(cfg Config) (chunk.IndexClient, error) {
	client, err := newES(cfg)
	if err != nil {
		return nil, err
	}
	indexClient := &esClient{
		cfg,
		client,
	}
	return indexClient, nil
}

func newES(cfg Config) (*elastic.Client, error) {
	//fix x509: certificate signed by unknown authority
	var tr *http.Transport
	if !strings.Contains(cfg.Address, "https") || cfg.TLSSkipVerify {
		tr = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	} else { // not skip TLS Verify
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
		caCert, err := ioutil.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// Setup HTTPS client
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()

		tr = &http.Transport{
			TLSClientConfig:     tlsConfig,
			TLSHandshakeTimeout: 5 * time.Second,
		}
	}

	httpClient := &http.Client{
		Timeout:   15 * time.Second,
		Transport: tr,
	}

	// Obtain a client and connect to the default Elasticsearch installation
	// on 127.0.0.1:9200. Of course you can configure your client to connect
	// to other hosts and configure it in various other ways.
	var err error
	client, err = elastic.NewClient(
		elastic.SetHttpClient(httpClient),
		// set basic auth for ElasticSearch which requires,
		// and is back-compatible for the one which does not require auth
		elastic.SetBasicAuth(cfg.Username, cfg.Password), elastic.SetURL(cfg.Address),
		elastic.SetSniff(false), elastic.SetHealthcheck(false))
	if err != nil {
		return nil, err
	}

	return client, nil
}
