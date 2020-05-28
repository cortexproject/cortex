package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func main() {
	var (
		cfg    cortex.Config
		asAt   string
		tenant string
	)
	configFile := parseConfigFileParameter(os.Args[1:])

	// This sets default values from flags to the config.
	// It needs to be called before parsing the config file!
	flagext.RegisterFlags(&cfg)

	if configFile != "" {
		if err := LoadConfig(configFile, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error loading config from %s: %v\n", configFile, err)
			os.Exit(1)
		}
	}

	// Ignore -config.file here, since it was already parsed, but it's still present on command line.
	flagext.IgnoredFlag(flag.CommandLine, configFileOption, "Configuration file to load.")

	flag.StringVar(&asAt, "as-at", "", "When to run the query as-at")
	flag.StringVar(&tenant, "tenant", "fake", "Tenant ID to query for")

	flag.Parse()

	util.InitLogger(&cfg.Server)
	err := cfg.Validate(util.Logger)
	if err != nil {
		fmt.Printf("error validating config: %v\n", err)
		os.Exit(1)
	}

	// 3 levels of stuff to initialize before we can get started
	overrides, err := validation.NewOverrides(cfg.LimitsConfig, nil)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to set up overrides", "err", err)
		os.Exit(1)
	}

	chunkStore, err := storage.NewStore(cfg.Storage, cfg.ChunkStore, cfg.Schema, overrides, nil, nil)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to set up chunk store", "err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	storeQueryable := querier.NewChunkStoreQueryable(cfg.Querier, chunkStore)
	_, engine := querier.New(cfg.Querier, noopQuerier{}, storeQueryable, nil, nil)

	if flag.NArg() != 1 {
		level.Error(util.Logger).Log("msg", "usage: oneshot <options> promql-query")
		os.Exit(1)
	}

	var asAtTime time.Time
	if asAt == "" {
		asAtTime = time.Now().Add(-1 * time.Minute)
	} else {
		asAtTime, err = time.Parse(time.RFC3339, asAt)
		if err != nil {
			level.Error(util.Logger).Log("msg", "invalid as-at time", "err", err)
			os.Exit(1)
		}
	}
	// Now execute the query
	query, err := engine.NewInstantQuery(storeQueryable, flag.Arg(0), asAtTime)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error in query", "err", err)
		os.Exit(1)
	}
	ctx := user.InjectOrgID(context.Background(), tenant)

	result := query.Exec(ctx)

	fmt.Printf("result: error %v %s\n", result.Err, result.Value)
}

const (
	configFileOption = "config.file"
)

// Parse -config.file option via separate flag set, to avoid polluting default one and calling flag.Parse on it twice.
func parseConfigFileParameter(args []string) (configFile string) {
	// ignore errors and any output here. Any flag errors will be reported by main flag.Parse() call.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	// usage not used in these functions.
	fs.StringVar(&configFile, configFileOption, "", "")

	// Try to find -config.file and -config.expand-env option in the flags. As Parsing stops on the first error, eg. unknown flag, we simply
	// try remaining parameters until we find config flag, or there are no params left.
	// (ContinueOnError just means that flag.Parse doesn't call panic or os.Exit, but it returns error, which we ignore)
	for len(args) > 0 {
		_ = fs.Parse(args)
		args = args[1:]
	}

	return
}

// LoadConfig read YAML-formatted config from filename into cfg.
func LoadConfig(filename string, cfg *cortex.Config) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	err = yaml.UnmarshalStrict(buf, cfg)
	if err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return nil
}

// Stub out distributor because we only want to query the store
type noopQuerier struct{}

func (n noopQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, nil
}

func (n noopQuerier) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return nil, nil
}

func (n noopQuerier) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, nil
}

func (n noopQuerier) LabelNames(context.Context) ([]string, error) {
	return nil, nil
}

func (n noopQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (n noopQuerier) MetricsMetadata(context.Context) ([]scrape.MetricMetadata, error) {
	return nil, nil
}
