package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"github.com/weaveworks/common/tracing"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/cortexproject/cortex/pkg/util"
)

func init() {
	prometheus.MustRegister(version.NewCollector("cortex"))
}

const configFileOption = "config.file"

func main() {
	var (
		eventSampleRate      int
		ballastBytes         int
		mutexProfileFraction int
	)

	cfg := parseAndLoadConfigFile()

	// Ignore -config.file here, since it was already parsed, but it's still present on command line.
	flagext.IgnoredFlag(flag.CommandLine, configFileOption, "Configuration file to load.")
	flag.IntVar(&eventSampleRate, "event.sample-rate", 0, "How often to sample observability events (0 = never).")
	flag.IntVar(&ballastBytes, "mem-ballast-size-bytes", 0, "Size of memory ballast to allocate.")
	flag.IntVar(&mutexProfileFraction, "debug.mutex-profile-fraction", 0, "Fraction at which mutex profile vents will be reported, 0 to disable")

	if mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	}

	flagext.RegisterFlags(&cfg)
	flag.Parse()

	// Validate the config once both the config file has been loaded
	// and CLI flags parsed.
	err := cfg.Validate()
	if err != nil {
		fmt.Printf("error validating config: %v\n", err)
		os.Exit(1)
	}

	// Allocate a block of memory to alter GC behaviour. See https://github.com/golang/go/issues/23044
	ballast := make([]byte, ballastBytes)

	util.InitLogger(&cfg.Server)
	util.InitEvents(eventSampleRate)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("cortex-" + cfg.Target.String())
	defer trace.Close()

	// Initialise seed for randomness usage.
	rand.Seed(time.Now().UnixNano())

	t, err := cortex.New(cfg)
	util.CheckFatal("initializing cortex", err)

	level.Info(util.Logger).Log("msg", "Starting Cortex", "version", version.Info())

	if err := t.Run(); err != nil {
		level.Error(util.Logger).Log("msg", "error running Cortex", "err", err)
	}

	runtime.KeepAlive(ballast)
	err = t.Stop()
	util.CheckFatal("initializing cortex", err)
}

// Parse -config.file option via separate flag set, to avoid polluting default one and calling flag.Parse on it twice.
func parseAndLoadConfigFile() cortex.Config {
	var configFile = ""
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError) // ignore unknown flags here.
	fs.SetOutput(ioutil.Discard)                            // eat all error messages for unknown flags, and default Usage output
	fs.StringVar(&configFile, configFileOption, "", "")     // usage not used in this function.
	_ = fs.Parse(os.Args[1:])

	var cfg cortex.Config
	if configFile != "" {
		if err := LoadConfig(configFile, &cfg); err != nil {
			fmt.Printf("error loading config from %s: %v\n", configFile, err)
			os.Exit(1)
		}
	}

	return cfg
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
