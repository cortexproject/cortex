package main

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	config_client "github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	maxLineWidth = 80
	tabWidth     = 2
)

var (
	// Ordered list of root blocks. The order is the same order that will
	// follow the markdown generation.
	rootBlocks = []rootBlock{
		{
			name:       "server_config",
			structType: reflect.TypeOf(server.Config{}),
			desc:       "The server_config configures the HTTP and gRPC server of the launched service(s).",
		},
		{
			name:       "distributor_config",
			structType: reflect.TypeOf(distributor.Config{}),
			desc:       "The distributor_config configures the Cortex distributor.",
		},
		{
			name:       "ingester_config",
			structType: reflect.TypeOf(ingester.Config{}),
			desc:       "The ingester_config configures the Cortex ingester.",
		},
		{
			name:       "querier_config",
			structType: reflect.TypeOf(querier.Config{}),
			desc:       "The querier_config configures the Cortex querier.",
		},
		{
			name:       "query_frontend_config",
			structType: reflect.TypeOf(frontend.Config{}),
			desc:       "The query_frontend_config configures the Cortex query-frontend.",
		},
		{
			name:       "queryrange_config",
			structType: reflect.TypeOf(queryrange.Config{}),
			desc:       "The queryrange_config configures the query splitting and caching in the Cortex query-frontend.",
		},
		{
			name:       "ruler_config",
			structType: reflect.TypeOf(ruler.Config{}),
			desc:       "The ruler_config configures the Cortex ruler.",
		},
		{
			name:       "alertmanager_config",
			structType: reflect.TypeOf(alertmanager.MultitenantAlertmanagerConfig{}),
			desc:       "The alertmanager_config configures the Cortex alertmanager.",
		},
		{
			name:       "table_manager_config",
			structType: reflect.TypeOf(chunk.TableManagerConfig{}),
			desc:       "The table_manager_config configures the Cortex table-manager.",
		},
		{
			name:       "storage_config",
			structType: reflect.TypeOf(storage.Config{}),
			desc:       "The storage_config configures where Cortex stores the data (chunks storage engine).",
		},
		{
			name:       "chunk_store_config",
			structType: reflect.TypeOf(chunk.StoreConfig{}),
			desc:       "The chunk_store_config configures how Cortex stores the data (chunks storage engine).",
		},
		{
			name:       "ingester_client_config",
			structType: reflect.TypeOf(client.Config{}),
			desc:       "The ingester_client_config configures how the Cortex distributors connect to the ingesters.",
		},
		{
			name:       "frontend_worker_config",
			structType: reflect.TypeOf(frontend.WorkerConfig{}),
			desc:       "The frontend_worker_config configures the worker - running within the Cortex querier - picking up and executing queries enqueued by the query-frontend.",
		},
		{
			name:       "etcd_config",
			structType: reflect.TypeOf(etcd.Config{}),
			desc:       "The etcd_config configures the etcd client.",
		},
		{
			name:       "consul_config",
			structType: reflect.TypeOf(consul.Config{}),
			desc:       "The consul_config configures the consul client.",
		},
		{
			name:       "memberlist_config",
			structType: reflect.TypeOf(memberlist.KVConfig{}),
			desc:       "The memberlist_config configures the Gossip memberlist.",
		},
		{
			name:       "limits_config",
			structType: reflect.TypeOf(validation.Limits{}),
			desc:       "The limits_config configures default and per-tenant limits imposed by Cortex services (ie. distributor, ingester, ...).",
		},
		{
			name:       "redis_config",
			structType: reflect.TypeOf(cache.RedisConfig{}),
			desc:       "The redis_config configures the Redis backend cache.",
		},
		{
			name:       "memcached_config",
			structType: reflect.TypeOf(cache.MemcachedConfig{}),
			desc:       "The memcached_config block configures how data is stored in Memcached (ie. expiration).",
		},
		{
			name:       "memcached_client_config",
			structType: reflect.TypeOf(cache.MemcachedClientConfig{}),
			desc:       "The memcached_client_config configures the client used to connect to Memcached.",
		},
		{
			name:       "fifo_cache_config",
			structType: reflect.TypeOf(cache.FifoCacheConfig{}),
			desc:       "The fifo_cache_config configures the local in-memory cache.",
		},
		{
			name:       "configdb_config",
			structType: reflect.TypeOf(db.Config{}),
			desc:       "The configdb_config configures the config database storing rules and alerts, and used by the 'configs' service to expose APIs to manage them.",
		},
		{
			name:       "configstore_config",
			structType: reflect.TypeOf(config_client.Config{}),
			desc:       "The configstore_config configures the config database storing rules and alerts, and is used by the Cortex alertmanager.",
		},
	}
)

func removeFlagPrefix(block *configBlock, prefix string) {
	for _, entry := range block.entries {
		switch entry.kind {
		case "block":
			// Skip root blocks
			if !entry.root {
				removeFlagPrefix(entry.block, prefix)
			}
		case "field":
			if strings.HasPrefix(entry.fieldFlag, prefix) {
				entry.fieldFlag = "<prefix>" + entry.fieldFlag[len(prefix):]
			}
		}
	}
}

func annotateFlagPrefix(blocks []*configBlock) {
	// Find duplicated blocks
	groups := map[string][]*configBlock{}
	for _, block := range blocks {
		groups[block.name] = append(groups[block.name], block)
	}

	// For each duplicated block, we need to fix the CLI flags, because
	// in the documentation each block will be displayed only once but
	// since they're duplicated they will have a different CLI flag
	// prefix, which we want to correctly document.
	for _, group := range groups {
		if len(group) == 1 {
			continue
		}

		// We need to find the CLI flags prefix of each config block. To do it,
		// we pick the first entry from each config block and then find the
		// different prefix across all of them.
		flags := []string{}
		for _, block := range group {
			for _, entry := range block.entries {
				if entry.kind == "field" {
					flags = append(flags, entry.fieldFlag)
					break
				}
			}
		}

		for i, prefix := range findFlagsPrefix(flags) {
			group[i].flagsPrefix = prefix
		}
	}

	// Finally, we can remove the CLI flags prefix from the blocks
	// which have one annotated.
	for _, block := range blocks {
		if block.flagsPrefix != "" {
			removeFlagPrefix(block, block.flagsPrefix)
		}
	}
}

func main() {
	cfg := &cortex.Config{}

	// In order to match YAML config fields with CLI flags, we do map
	// the memory address of the CLI flag variables and match them with
	// the config struct fields address.
	flags := parseFlags(cfg)

	// Parse the config, mapping each config field with the related CLI flag.
	blocks, err := parseConfig(nil, cfg, flags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while generating the doc: %s\n", err.Error())
		os.Exit(1)
	}

	// Annotate the flags prefix for each root block, and remove the
	// prefix wherever encountered in the config blocks.
	annotateFlagPrefix(blocks)

	// Generate markdown
	md := &markdownWriter{}
	md.writeConfigDoc(blocks)
	fmt.Println(md.string())
}
