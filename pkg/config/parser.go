package config

import (
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/compactor"
	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/client"
	config_client "github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/flusher"
	"github.com/cortexproject/cortex/pkg/frontend"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	querier_worker "github.com/cortexproject/cortex/pkg/querier/worker"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	yamlFieldNameParser   = regexp.MustCompile("^[^,]+")
	yamlFieldInlineParser = regexp.MustCompile("^[^,]*,inline$")

	// Ordered list of root blocks. The order is the same order that will
	// follow the markdown generation.
	RootBlocks = []RootBlock{
		{
			Name:       "server_config",
			structType: reflect.TypeOf(server.Config{}),
			desc:       "The server_config configures the HTTP and gRPC server of the launched service(s).",
		},
		{
			Name:       "distributor_config",
			structType: reflect.TypeOf(distributor.Config{}),
			desc:       "The distributor_config configures the Cortex distributor.",
		},
		{
			Name:       "ingester_config",
			structType: reflect.TypeOf(ingester.Config{}),
			desc:       "The ingester_config configures the Cortex ingester.",
		},
		{
			Name:       "querier_config",
			structType: reflect.TypeOf(querier.Config{}),
			desc:       "The querier_config configures the Cortex querier.",
		},
		{
			Name:       "query_frontend_config",
			structType: reflect.TypeOf(frontend.CombinedFrontendConfig{}),
			desc:       "The query_frontend_config configures the Cortex query-frontend.",
		},
		{
			Name:       "query_range_config",
			structType: reflect.TypeOf(queryrange.Config{}),
			desc:       "The query_range_config configures the query splitting and caching in the Cortex query-frontend.",
		},
		{
			Name:       "ruler_config",
			structType: reflect.TypeOf(ruler.Config{}),
			desc:       "The ruler_config configures the Cortex ruler.",
		},
		{
			Name:       "alertmanager_config",
			structType: reflect.TypeOf(alertmanager.MultitenantAlertmanagerConfig{}),
			desc:       "The alertmanager_config configures the Cortex alertmanager.",
		},
		{
			Name:       "table_manager_config",
			structType: reflect.TypeOf(chunk.TableManagerConfig{}),
			desc:       "The table_manager_config configures the Cortex table-manager.",
		},
		{
			Name:       "storage_config",
			structType: reflect.TypeOf(storage.Config{}),
			desc:       "The storage_config configures where Cortex stores the data (chunks storage engine).",
		},
		{
			Name:       "flusher_config",
			structType: reflect.TypeOf(flusher.Config{}),
			desc:       "The flusher_config configures the WAL flusher target, used to manually run one-time flushes when scaling down ingesters.",
		},
		{
			Name:       "chunk_store_config",
			structType: reflect.TypeOf(chunk.StoreConfig{}),
			desc:       "The chunk_store_config configures how Cortex stores the data (chunks storage engine).",
		},
		{
			Name:       "ingester_client_config",
			structType: reflect.TypeOf(client.Config{}),
			desc:       "The ingester_client_config configures how the Cortex distributors connect to the ingesters.",
		},
		{
			Name:       "frontend_worker_config",
			structType: reflect.TypeOf(querier_worker.Config{}),
			desc:       "The frontend_worker_config configures the worker - running within the Cortex querier - picking up and executing queries enqueued by the query-frontend or query-scheduler.",
		},
		{
			Name:       "etcd_config",
			structType: reflect.TypeOf(etcd.Config{}),
			desc:       "The etcd_config configures the etcd client.",
		},
		{
			Name:       "consul_config",
			structType: reflect.TypeOf(consul.Config{}),
			desc:       "The consul_config configures the consul client.",
		},
		{
			Name:       "memberlist_config",
			structType: reflect.TypeOf(memberlist.KVConfig{}),
			desc:       "The memberlist_config configures the Gossip memberlist.",
		},
		{
			Name:       "limits_config",
			structType: reflect.TypeOf(validation.Limits{}),
			desc:       "The limits_config configures default and per-tenant limits imposed by Cortex services (ie. distributor, ingester, ...).",
		},
		{
			Name:       "redis_config",
			structType: reflect.TypeOf(cache.RedisConfig{}),
			desc:       "The redis_config configures the Redis backend cache.",
		},
		{
			Name:       "memcached_config",
			structType: reflect.TypeOf(cache.MemcachedConfig{}),
			desc:       "The memcached_config block configures how data is stored in Memcached (ie. expiration).",
		},
		{
			Name:       "memcached_client_config",
			structType: reflect.TypeOf(cache.MemcachedClientConfig{}),
			desc:       "The memcached_client_config configures the client used to connect to Memcached.",
		},
		{
			Name:       "fifo_cache_config",
			structType: reflect.TypeOf(cache.FifoCacheConfig{}),
			desc:       "The fifo_cache_config configures the local in-memory cache.",
		},
		{
			Name:       "configs_config",
			structType: reflect.TypeOf(configs.Config{}),
			desc:       "The configs_config configures the Cortex Configs DB and API.",
		},
		{
			Name:       "configstore_config",
			structType: reflect.TypeOf(config_client.Config{}),
			desc:       "The configstore_config configures the config database storing rules and alerts, and is used by the Cortex alertmanager.",
		},
		{
			Name:       "blocks_storage_config",
			structType: reflect.TypeOf(tsdb.BlocksStorageConfig{}),
			desc:       "The blocks_storage_config configures the blocks storage.",
		},
		{
			Name:       "compactor_config",
			structType: reflect.TypeOf(compactor.Config{}),
			desc:       "The compactor_config configures the compactor for the blocks storage.",
		},
		{
			Name:       "store_gateway_config",
			structType: reflect.TypeOf(storegateway.Config{}),
			desc:       "The store_gateway_config configures the store-gateway service used by the blocks storage.",
		},
		{
			Name:       "purger_config",
			structType: reflect.TypeOf(purger.Config{}),
			desc:       "The purger_config configures the purger which takes care of delete requests",
		},
	}
)

type ConfigBlock struct {
	Name          string
	Desc          string
	Entries       []*ConfigEntry
	FlagsPrefix   string
	FlagsPrefixes []string
}

func (b *ConfigBlock) Add(entry *ConfigEntry) {
	b.Entries = append(b.Entries, entry)
}

type ConfigEntry struct {
	Kind     string
	Name     string
	Required bool

	// In case the kind is "block"
	Block     *ConfigBlock
	BlockDesc string
	Root      bool

	// In case the kind is "field"
	FieldFlag    string
	FieldDesc    string
	FieldType    string
	FieldDefault string
}

type RootBlock struct {
	Name       string
	desc       string
	structType reflect.Type
}

func ParseFlags(cfg flagext.Registerer) map[uintptr]*flag.Flag {
	fs := flag.NewFlagSet("", flag.PanicOnError)
	cfg.RegisterFlags(fs)

	flags := map[uintptr]*flag.Flag{}
	fs.VisitAll(func(f *flag.Flag) {
		// Skip deprecated flags
		if f.Value.String() == "deprecated" {
			return
		}

		ptr := reflect.ValueOf(f.Value).Pointer()
		flags[ptr] = f
	})

	return flags
}

func ParseConfig(block *ConfigBlock, cfg interface{}, flags map[uintptr]*flag.Flag) ([]*ConfigBlock, error) {
	blocks := []*ConfigBlock{}

	// If the input block is nil it means we're generating the doc for the top-level block
	if block == nil {
		block = &ConfigBlock{}
		blocks = append(blocks, block)
	}

	// The input config is expected to be addressable.
	if reflect.TypeOf(cfg).Kind() != reflect.Ptr {
		t := reflect.TypeOf(cfg)
		return nil, fmt.Errorf("%s is a %s while a %s is expected", t, t.Kind(), reflect.Ptr)
	}

	// The input config is expected to be a pointer to struct.
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is a %s while a %s is expected", v, v.Kind(), reflect.Struct)
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.FieldByIndex(field.Index)

		// Skip fields explicitly marked as "hidden" in the doc
		if isFieldHidden(field) {
			continue
		}

		// Skip fields not exported via yaml (unless they're inline)
		fieldName := getFieldName(field)
		if fieldName == "" && !isFieldInline(field) {
			continue
		}

		// Skip field types which are non configurable
		if field.Type.Kind() == reflect.Func {
			continue
		}

		// Skip deprecated fields we're still keeping for backward compatibility
		// reasons (by convention we prefix them by UnusedFlag)
		if strings.HasPrefix(field.Name, "UnusedFlag") {
			continue
		}

		// Handle custom fields in vendored libs upon which we have no control.
		fieldEntry, err := getCustomFieldEntry(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}
		if fieldEntry != nil {
			block.Add(fieldEntry)
			continue
		}

		// Recursively re-iterate if it's a struct
		if field.Type.Kind() == reflect.Struct {
			// Check whether the sub-block is a root config block
			rootName, rootDesc, isRoot := isRootBlock(field.Type)

			// Since we're going to recursively iterate, we need to create a new sub
			// block and pass it to the doc generation function.
			var subBlock *ConfigBlock

			if !isFieldInline(field) {
				var blockName string
				var blockDesc string

				if isRoot {
					blockName = rootName
					blockDesc = rootDesc
				} else {
					blockName = fieldName
					blockDesc = getFieldDescription(field, "")
				}

				subBlock = &ConfigBlock{
					Name: blockName,
					Desc: blockDesc,
				}

				block.Add(&ConfigEntry{
					Kind:      "block",
					Name:      fieldName,
					Required:  isFieldRequired(field),
					Block:     subBlock,
					BlockDesc: blockDesc,
					Root:      isRoot,
				})

				if isRoot {
					blocks = append(blocks, subBlock)
				}
			} else {
				subBlock = block
			}

			// Recursively generate the doc for the sub-block
			otherBlocks, err := ParseConfig(subBlock, fieldValue.Addr().Interface(), flags)
			if err != nil {
				return nil, err
			}

			blocks = append(blocks, otherBlocks...)
			continue
		}

		fieldType, err := getFieldType(field.Type)
		if err != nil {
			return nil, errors.Wrapf(err, "config=%s.%s", t.PkgPath(), t.Name())
		}

		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, errors.Wrapf(err, "config=%s.%s", t.PkgPath(), t.Name())
		}
		if fieldFlag == nil {
			block.Add(&ConfigEntry{
				Kind:      "field",
				Name:      fieldName,
				Required:  isFieldRequired(field),
				FieldDesc: getFieldDescription(field, ""),
				FieldType: fieldType,
			})
			continue
		}

		block.Add(&ConfigEntry{
			Kind:         "field",
			Name:         fieldName,
			Required:     isFieldRequired(field),
			FieldFlag:    fieldFlag.Name,
			FieldDesc:    getFieldDescription(field, fieldFlag.Usage),
			FieldType:    fieldType,
			FieldDefault: fieldFlag.DefValue,
		})
	}

	return blocks, nil
}

func getFieldName(field reflect.StructField) string {
	name := field.Name
	tag := field.Tag.Get("yaml")

	// If the tag is not specified, then an exported field can be
	// configured via the field name (lowercase), while an unexported
	// field can't be configured.
	if tag == "" {
		if unicode.IsLower(rune(name[0])) {
			return ""
		}

		return strings.ToLower(name)
	}

	// Parse the field name
	fieldName := yamlFieldNameParser.FindString(tag)
	if fieldName == "-" {
		return ""
	}

	return fieldName
}

func getFieldType(t reflect.Type) (string, error) {
	// Handle custom data types used in the config
	switch t.String() {
	case "*url.URL":
		return "url", nil
	case "time.Duration":
		return "duration", nil
	case "cortex.moduleName":
		return "string", nil
	case "flagext.StringSliceCSV":
		return "string", nil
	case "[]*relabel.Config":
		return "relabel_config...", nil
	}

	// Fallback to auto-detection of built-in data types
	switch t.Kind() {
	case reflect.Bool:
		return "boolean", nil

	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return "int", nil

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return "float", nil

	case reflect.String:
		return "string", nil

	case reflect.Slice:
		// Get the type of elements
		elemType, err := getFieldType(t.Elem())
		if err != nil {
			return "", err
		}

		return "list of " + elemType, nil

	case reflect.Map:
		return fmt.Sprintf("map of %s to %s", t.Key(), t.Elem().String()), nil

	default:
		return "", fmt.Errorf("unsupported data type %s", t.Kind())
	}
}

func getFieldFlag(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag) (*flag.Flag, error) {
	if isAbsentInCLI(field) {
		return nil, nil
	}
	fieldPtr := fieldValue.Addr().Pointer()
	fieldFlag, ok := flags[fieldPtr]
	if !ok {
		return nil, fmt.Errorf("unable to find CLI flag for '%s' config entry", field.Name)
	}

	return fieldFlag, nil
}

func getCustomFieldEntry(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag) (*ConfigEntry, error) {
	if field.Type == reflect.TypeOf(logging.Level{}) || field.Type == reflect.TypeOf(logging.Format{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &ConfigEntry{
			Kind:         "field",
			Name:         getFieldName(field),
			Required:     isFieldRequired(field),
			FieldFlag:    fieldFlag.Name,
			FieldDesc:    fieldFlag.Usage,
			FieldType:    "string",
			FieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(flagext.URLValue{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &ConfigEntry{
			Kind:         "field",
			Name:         getFieldName(field),
			Required:     isFieldRequired(field),
			FieldFlag:    fieldFlag.Name,
			FieldDesc:    fieldFlag.Usage,
			FieldType:    "url",
			FieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(flagext.Secret{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &ConfigEntry{
			Kind:         "field",
			Name:         getFieldName(field),
			Required:     isFieldRequired(field),
			FieldFlag:    fieldFlag.Name,
			FieldDesc:    fieldFlag.Usage,
			FieldType:    "string",
			FieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(model.Duration(0)) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &ConfigEntry{
			Kind:         "field",
			Name:         getFieldName(field),
			Required:     isFieldRequired(field),
			FieldFlag:    fieldFlag.Name,
			FieldDesc:    fieldFlag.Usage,
			FieldType:    "duration",
			FieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(flagext.Time{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &ConfigEntry{
			Kind:         "field",
			Name:         getFieldName(field),
			Required:     isFieldRequired(field),
			FieldFlag:    fieldFlag.Name,
			FieldDesc:    fieldFlag.Usage,
			FieldType:    "time",
			FieldDefault: fieldFlag.DefValue,
		}, nil
	}

	return nil, nil
}

func isFieldHidden(f reflect.StructField) bool {
	return getDocTagFlag(f, "hidden")
}

func isAbsentInCLI(f reflect.StructField) bool {
	return getDocTagFlag(f, "nocli")
}

func isFieldRequired(f reflect.StructField) bool {
	return getDocTagFlag(f, "required")
}

func isFieldInline(f reflect.StructField) bool {
	return yamlFieldInlineParser.MatchString(f.Tag.Get("yaml"))
}

func getFieldDescription(f reflect.StructField, fallback string) string {
	if desc := getDocTagValue(f, "description"); desc != "" {
		return desc
	}

	return fallback
}

func isRootBlock(t reflect.Type) (string, string, bool) {
	for _, rootBlock := range RootBlocks {
		if t == rootBlock.structType {
			return rootBlock.Name, rootBlock.desc, true
		}
	}

	return "", "", false
}

func getDocTagFlag(f reflect.StructField, name string) bool {
	cfg := parseDocTag(f)
	_, ok := cfg[name]
	return ok
}

func getDocTagValue(f reflect.StructField, name string) string {
	cfg := parseDocTag(f)
	return cfg[name]
}

func parseDocTag(f reflect.StructField) map[string]string {
	cfg := map[string]string{}
	tag := f.Tag.Get("doc")

	if tag == "" {
		return cfg
	}

	for _, entry := range strings.Split(tag, "|") {
		parts := strings.SplitN(entry, "=", 2)

		switch len(parts) {
		case 1:
			cfg[parts[0]] = ""
		case 2:
			cfg[parts[0]] = parts[1]
		}
	}

	return cfg
}
