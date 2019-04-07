package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"github.com/weaveworks/common/tracing"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func init() {
	prometheus.MustRegister(version.NewCollector("loki"))
}

func main() {
	var (
		cfg             cortex.Config
		configFile      = ""
		eventSampleRate int
	)
	flag.StringVar(&configFile, "config.file", "", "Configuration file to load.")
	flag.IntVar(&eventSampleRate, "event.sample-rate", 0, "How often to sample observability events (0 = never).")

	flagext.RegisterFlags(&cfg)
	flag.Parse()

	if configFile != "" {
		if err := LoadConfig(configFile, &cfg); err != nil {
			fmt.Printf("error loading config from %s: %v\n", configFile, err)
			os.Exit(1)
		}
	}

	// Parse a second time, as command line flags should take precedent over the config file.
	flag.Parse()

	util.InitLogger(&cfg.Server)
	util.InitEvents(eventSampleRate)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv(cfg.Target.String())
	defer trace.Close()

	t, err := cortex.New(cfg)
	util.CheckFatal("initializing cortex", err)

	level.Info(util.Logger).Log("msg", "Starting Cortex", "version", version.Info())

	if err := t.Run(); err != nil {
		level.Error(util.Logger).Log("msg", "error running Cortex", "err", err)
	}

	err = t.Stop()
	util.CheckFatal("initializing cortex", err)
}

// LoadConfig read YAML-formatted config from filename into cfg.
func LoadConfig(filename string, cfg interface{}) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	return yaml.UnmarshalStrict(buf, cfg)
}
