package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/server"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/tools/thanosconvert"
)

var (
	configFilename string
	dryRun         bool
)

func main() {
	serverConfig := server.Config{}
	serverConfig.RegisterFlags(flag.CommandLine)
	flag.StringVar(&configFilename, "config", "", "Path to bucket config YAML")
	flag.BoolVar(&dryRun, "dry-run", false, "Don't make changes; only report what needs to be done")
	flag.Parse()

	util.InitLogger(&serverConfig)

	if configFilename == "" {
		level.Error(util.Logger).Log("msg", "-config is required")
		os.Exit(1)
	}

	registry := prometheus.DefaultRegisterer

	ctx := context.Background()

	cfg := bucket.Config{}

	buf, err := ioutil.ReadFile(configFilename)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to load config file", "err", err, "filename", configFilename)
		os.Exit(1)
	}
	err = yaml.UnmarshalStrict(buf, &cfg)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to parse config", "err", err)
		os.Exit(1)
	}

	converter, err := thanosconvert.NewThanosBlockConverter(ctx, cfg, dryRun, util.Logger, registry)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to initialize", "err", err)
		os.Exit(1)
	}

	iterCtx := context.Background()
	results, err := converter.Run(iterCtx)
	if err != nil {
		level.Error(util.Logger).Log("msg", "iterate blocks", "err", err)
		os.Exit(1)
	}

	fmt.Println("Results:")
	for user, res := range results {
		fmt.Printf("User %s:\n", user)
		fmt.Printf("  Converted %d:\n  %s", len(res.ConvertedBlocks), strings.Join(res.ConvertedBlocks, ","))
		fmt.Printf("  Unchanged %d:\n  %s", len(res.UnchangedBlocks), strings.Join(res.UnchangedBlocks, ","))
		fmt.Printf("  Failed %d:\n  %s", len(res.FailedBlocks), strings.Join(res.FailedBlocks, ","))
	}

}
