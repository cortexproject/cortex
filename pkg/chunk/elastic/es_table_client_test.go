// +build elastic

package elastic

// There's no good embedded ElasticSearch, so we use a real ElasticSearch instance.
// To enable below tests:
// $ docker pull docker.elastic.co/elasticsearch/elasticsearch:6.4.3
// $ docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.4.3

import (
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"testing"
)

func TestCRDTable(t *testing.T) {
	client, _ := NewTableClient(ctx, config)
	result, _  := client.ListTables(ctx)
	fmt.Println(result)

	desc := chunk.TableDesc{
		Name:              "test1",
		UseOnDemandIOMode: false,
		ProvisionedRead:   0,
		ProvisionedWrite:  0,
		Tags:              nil,
		WriteScale:        chunk.AutoScalingConfig{},
		ReadScale:         chunk.AutoScalingConfig{},
	}
	client.CreateTable(ctx, desc)

	desc2 := chunk.TableDesc{
		Name:              "test2",
		UseOnDemandIOMode: false,
		ProvisionedRead:   0,
		ProvisionedWrite:  0,
		Tags:              nil,
		WriteScale:        chunk.AutoScalingConfig{},
		ReadScale:         chunk.AutoScalingConfig{},
	}
	client.CreateTable(ctx, desc2)
	result, _  = client.ListTables(ctx)
	fmt.Println(result)

	client.DeleteTable(ctx, "test1")

	result, _  = client.ListTables(ctx)
	fmt.Println(result)
}
