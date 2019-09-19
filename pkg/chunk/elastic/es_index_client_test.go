// +build elastic

package elastic

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/cortexproject/cortex/pkg/chunk"
	"testing"
)

// There's no good embedded ElasticSearch, so we use a real ElasticSearch instance.
// To enable below tests:
// $ docker pull docker.elastic.co/elasticsearch/elasticsearch:6.4.3
// $ docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.4.3

var config = Config{
	Address:   "http://127.0.0.1:9200",
	IndexType: "cortexindex",
	TemplateName: "cortex-template",
	NumOfShards: 1,
	NumOfReplicas: 0,
	TLSSkipVerify: true,
	Username:  "user",
	Password:  "pwd",
	MaxFetchDocs: 5,
}

func TestNewESIndexClient(t *testing.T) {
	NewESIndexClient(config)
}

func TestCreate(t *testing.T) {
	client, _ := NewESIndexClient(config)
	ctx = context.Background()

	writeBatch := client.NewWriteBatch()
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job1", nil, nil)
	writeBatch.Add("cortexindex_2594_test", "fake:d18161:logs:job12", nil, nil)

	// create template
	tableClient, _ := NewTableClient(ctx, config)
	tableClient.CreateTable(ctx, chunk.TableDesc{})
	client.BatchWrite(ctx, writeBatch)
}

func TestQuery(t *testing.T) {
	client, _ := NewESIndexClient(config)
	ctx = context.Background()

	var have int
	queries := []chunk.IndexQuery {{
		TableName:        "cortexindex_2594",
		HashValue:        "fake:d18162:logs:job",
		RangeValuePrefix: nil,
	}, {
		TableName:        "cortexindex_2594_test",
		HashValue:        "fake:d18161:logs:job1",
		RangeValuePrefix: nil,
	}}
	client.QueryPages(ctx, queries, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
		iter := read.Iterator()
		for iter.Next() {
			have++
		}
		return true
	})
	// we create 11 matched entries above, and use max fetch 5 to test search after for pagination
	assert.Equal(t, 11, have)
}

