package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"

	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/compactor"
	"github.com/cortexproject/cortex/pkg/configs"
	config_client "github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/flusher"
	"github.com/cortexproject/cortex/pkg/frontend"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	querier_worker "github.com/cortexproject/cortex/pkg/querier/worker"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway"
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
			structType: reflect.TypeOf(frontend.CombinedFrontendConfig{}),
			desc:       "The query_frontend_config configures the Cortex query-frontend.",
		},
		{
			name:       "query_range_config",
			structType: reflect.TypeOf(queryrange.Config{}),
			desc:       "The query_range_config configures the query splitting and caching in the Cortex query-frontend.",
		},
		{
			name:       "ruler_config",
			structType: reflect.TypeOf(ruler.Config{}),
			desc:       "The ruler_config configures the Cortex ruler.",
		},
		{
			name:       "ruler_storage_config",
			structType: reflect.TypeOf(rulestore.Config{}),
			desc:       "The ruler_storage_config configures the Cortex ruler storage backend.",
		},
		{
			name:       "alertmanager_config",
			structType: reflect.TypeOf(alertmanager.MultitenantAlertmanagerConfig{}),
			desc:       "The alertmanager_config configures the Cortex alertmanager.",
		},
		{
			name:       "alertmanager_storage_config",
			structType: reflect.TypeOf(alertstore.Config{}),
			desc:       "The alertmanager_storage_config configures the Cortex alertmanager storage backend.",
		},
		{
			name:       "storage_config",
			structType: reflect.TypeOf(storage.Config{}),
			desc:       "The storage_config configures where Cortex stores the data (chunks storage engine).",
		},
		{
			name:       "flusher_config",
			structType: reflect.TypeOf(flusher.Config{}),
			desc:       "The flusher_config configures the WAL flusher target, used to manually run one-time flushes when scaling down ingesters.",
		},
		{
			name:       "ingester_client_config",
			structType: reflect.TypeOf(client.Config{}),
			desc:       "The ingester_client_config configures how the Cortex distributors connect to the ingesters.",
		},
		{
			name:       "frontend_worker_config",
			structType: reflect.TypeOf(querier_worker.Config{}),
			desc:       "The frontend_worker_config configures the worker - running within the Cortex querier - picking up and executing queries enqueued by the query-frontend or query-scheduler.",
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
			name:       "configs_config",
			structType: reflect.TypeOf(configs.Config{}),
			desc:       "The configs_config configures the Cortex Configs DB and API.",
		},
		{
			name:       "configstore_config",
			structType: reflect.TypeOf(config_client.Config{}),
			desc:       "The configstore_config configures the config database storing rules and alerts, and is used by the Cortex alertmanager.",
		},
		{
			name:       "blocks_storage_config",
			structType: reflect.TypeOf(tsdb.BlocksStorageConfig{}),
			desc:       "The blocks_storage_config configures the blocks storage.",
		},
		{
			name:       "compactor_config",
			structType: reflect.TypeOf(compactor.Config{}),
			desc:       "The compactor_config configures the compactor for the blocks storage.",
		},
		{
			name:       "store_gateway_config",
			structType: reflect.TypeOf(storegateway.Config{}),
			desc:       "The store_gateway_config configures the store-gateway service used by the blocks storage.",
		},
		{
			name:       "s3_sse_config",
			structType: reflect.TypeOf(s3.SSEConfig{}),
			desc:       "The s3_sse_config configures the S3 server-side encryption.",
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

		allPrefixes := []string{}
		for i, prefix := range findFlagsPrefix(flags) {
			group[i].flagsPrefix = prefix
			allPrefixes = append(allPrefixes, prefix)
		}

		// Store all found prefixes into each block so that when we generate the
		// markdown we also know which are all the prefixes for each root block.
		for _, block := range group {
			block.flagsPrefixes = allPrefixes
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

func generateBlocksMarkdown(blocks []*configBlock) string {
	md := &markdownWriter{}
	md.writeConfigDoc(blocks)
	return md.string()
}

func generateBlockMarkdown(blocks []*configBlock, blockName, fieldName string) string {
	// Look for the requested block.
	for _, block := range blocks {
		if block.name != blockName {
			continue
		}

		md := &markdownWriter{}

		// Wrap the root block with another block, so that we can show the name of the
		// root field containing the block specs.
		md.writeConfigBlock(&configBlock{
			name: blockName,
			desc: block.desc,
			entries: []*configEntry{
				{
					kind:      "block",
					name:      fieldName,
					required:  true,
					block:     block,
					blockDesc: "",
					root:      false,
				},
			},
		})

		return md.string()
	}

	// If the block has not been found, we return an empty string.
	return ""
}

func main() {
	// Parse the generator flags.
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: doc-generator template-file")
		os.Exit(1)
	}

	templatePath := flag.Arg(0)

	// In order to match YAML config fields with CLI flags, we do map
	// the memory address of the CLI flag variables and match them with
	// the config struct fields address.
	cfg := &cortex.Config{}
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

	// Generate documentation markdown.
	data := struct {
		ConfigFile               string
		BlocksStorageConfigBlock string
		StoreGatewayConfigBlock  string
		CompactorConfigBlock     string
		QuerierConfigBlock       string
		S3SSEConfigBlock         string
		GeneratedFileWarning     string
	}{
		ConfigFile:               generateBlocksMarkdown(blocks),
		BlocksStorageConfigBlock: generateBlockMarkdown(blocks, "blocks_storage_config", "blocks_storage"),
		StoreGatewayConfigBlock:  generateBlockMarkdown(blocks, "store_gateway_config", "store_gateway"),
		CompactorConfigBlock:     generateBlockMarkdown(blocks, "compactor_config", "compactor"),
		QuerierConfigBlock:       generateBlockMarkdown(blocks, "querier_config", "querier"),
		S3SSEConfigBlock:         generateBlockMarkdown(blocks, "s3_sse_config", "sse"),
		GeneratedFileWarning:     "<!-- DO NOT EDIT THIS FILE - This file has been automatically generated from its .template -->",
	}

	// Load the template file.
	tpl := template.New(filepath.Base(templatePath))
	tpl, err = tpl.ParseFiles(templatePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while loading the template %s: %s\n", templatePath, err.Error())
		os.Exit(1)
	}

	// Execute the template to inject generated doc.
	if err := tpl.Execute(os.Stdout, data); err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while executing the template %s: %s\n", templatePath, err.Error())
		os.Exit(1)
	}
}
