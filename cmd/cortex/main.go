package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

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
	prometheus.MustRegister(version.NewCollector("cortex"))
}

const (
	configFileOption = "config.file"
	configExpandENV  = "config.expand-env"
)

var testMode = false

func main() {
	var (
		cfg                  cortex.Config
		eventSampleRate      int
		ballastBytes         int
		mutexProfileFraction int
	)

	configFile, expandENV := parseConfigFileParameter(os.Args[1:])

	// This sets default values from flags to the config.
	// It needs to be called before parsing the config file!
	flagext.RegisterFlags(&cfg)

	if configFile != "" {
		if err := LoadConfig(configFile, expandENV, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error loading config from %s: %v\n", configFile, err)
			if testMode {
				return
			}
			os.Exit(1)
		}
	}

	// Ignore -config.file and -config.expand-env here, since it was already parsed, but it's still present on command line.
	flagext.IgnoredFlag(flag.CommandLine, configFileOption, "Configuration file to load.")
	flagext.IgnoredFlag(flag.CommandLine, configExpandENV, "Expands ${var} or $var in config according to the values of the environment variables.")

	flag.IntVar(&eventSampleRate, "event.sample-rate", 0, "How often to sample observability events (0 = never).")
	flag.IntVar(&ballastBytes, "mem-ballast-size-bytes", 0, "Size of memory ballast to allocate.")
	flag.IntVar(&mutexProfileFraction, "debug.mutex-profile-fraction", 0, "Fraction at which mutex profile vents will be reported, 0 to disable")

	if testMode {
		// Don't exit on error in test mode. Just parse parameters, dump config and stop.
		flag.CommandLine.Init(flag.CommandLine.Name(), flag.ContinueOnError)
		flag.Parse()
		DumpYaml(&cfg)
		return
	}

	flag.Parse()

	if mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	}

	util.InitLogger(&cfg.Server)
	// Validate the config once both the config file has been loaded
	// and CLI flags parsed.
	err := cfg.Validate(util.Logger)
	if err != nil {
		fmt.Printf("error validating config: %v\n", err)
		os.Exit(1)
	}

	// Allocate a block of memory to alter GC behaviour. See https://github.com/golang/go/issues/23044
	ballast := make([]byte, ballastBytes)

	util.InitEvents(eventSampleRate)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	if trace, err := tracing.NewFromEnv("cortex-" + cfg.Target.String()); err != nil {
		level.Error(util.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
	} else {
		defer trace.Close()
	}

	// Initialise seed for randomness usage.
	rand.Seed(time.Now().UnixNano())

	t, err := cortex.New(cfg)
	util.CheckFatal("initializing cortex", err)

	level.Info(util.Logger).Log("msg", "Starting Cortex", "version", version.Info())

	err = t.Run()

	runtime.KeepAlive(ballast)
	util.CheckFatal("running cortex", err)
}

// Parse -config.file and -config.expand-env option via separate flag set, to avoid polluting default one and calling flag.Parse on it twice.
func parseConfigFileParameter(args []string) (configFile string, expandEnv bool) {
	// ignore errors and any output here. Any flag errors will be reported by main flag.Parse() call.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	// usage not used in these functions.
	fs.StringVar(&configFile, configFileOption, "", "")
	fs.BoolVar(&expandEnv, configExpandENV, false, "")

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
func LoadConfig(filename string, expandENV bool, cfg *cortex.Config) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	if expandENV {
		buf = expandEnv(buf)
	}

	err = yaml.UnmarshalStrict(buf, cfg)
	if err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return nil
}

func DumpYaml(cfg *cortex.Config) {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	} else {
		fmt.Printf("%s\n", out)
	}
}

// expandEnv replaces ${var} or $var in config according to the values of the current environment variables.
// The replacement is case-sensitive. References to undefined variables are replaced by the empty string.
// A default value can be given by using the form ${var:default value}.
func expandEnv(config []byte) []byte {
	return []byte(os.Expand(string(config), func(key string) string {
		keyAndDefault := strings.SplitN(key, ":", 2)
		key = keyAndDefault[0]

		v := os.Getenv(key)
		if v == "" && len(keyAndDefault) == 2 {
			v = keyAndDefault[1] // Set value to the default.
		}
		return v
	}))
}
