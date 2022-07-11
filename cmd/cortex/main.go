package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"github.com/weaveworks/common/tracing"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

// Version is set via build flag -ldflags -X main.Version
var (
	Version  string
	Branch   string
	Revision string
)

// configHash exposes information about the loaded config
var configHash *prometheus.GaugeVec = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "cortex_config_hash",
		Help: "Hash of the currently active config file.",
	},
	[]string{"sha256"},
)

func init() {
	version.Version = Version
	version.Branch = Branch
	version.Revision = Revision
	prometheus.MustRegister(version.NewCollector("cortex"))
	prometheus.MustRegister(configHash)
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
		blockProfileRate     int
		printVersion         bool
		printModules         bool
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
	_ = flag.CommandLine.Bool(configExpandENV, false, "Expands ${var} or $var in config according to the values of the environment variables.")

	flag.IntVar(&eventSampleRate, "event.sample-rate", 0, "How often to sample observability events (0 = never).")
	flag.IntVar(&ballastBytes, "mem-ballast-size-bytes", 0, "Size of memory ballast to allocate.")
	flag.IntVar(&mutexProfileFraction, "debug.mutex-profile-fraction", 0, "Fraction of mutex contention events that are reported in the mutex profile. On average 1/rate events are reported. 0 to disable.")
	flag.IntVar(&blockProfileRate, "debug.block-profile-rate", 0, "Fraction of goroutine blocking events that are reported in the blocking profile. 1 to include every blocking event in the profile, 0 to disable.")
	flag.BoolVar(&printVersion, "version", false, "Print Cortex version and exit.")
	flag.BoolVar(&printModules, "modules", false, "List available values that can be used as target.")

	usage := flag.CommandLine.Usage
	flag.CommandLine.Usage = func() { /* don't do anything by default, we will print usage ourselves, but only when requested. */ }
	flag.CommandLine.Init(flag.CommandLine.Name(), flag.ContinueOnError)

	err := flag.CommandLine.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		// Print available parameters to stdout, so that users can grep/less it easily.
		flag.CommandLine.SetOutput(os.Stdout)
		usage()
		if !testMode {
			os.Exit(2)
		}
	} else if err != nil {
		fmt.Fprintln(flag.CommandLine.Output(), "Run with -help to get list of available parameters")
		if !testMode {
			os.Exit(2)
		}
	}

	if printVersion {
		fmt.Fprintln(os.Stdout, version.Print("Cortex"))
		return
	}

	// Validate the config once both the config file has been loaded
	// and CLI flags parsed.
	err = cfg.Validate(util_log.Logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error validating config: %v\n", err)
		if !testMode {
			os.Exit(1)
		}
	}

	// Continue on if -modules flag is given. Code handling the
	// -modules flag will not start cortex.
	if testMode && !printModules {
		DumpYaml(&cfg)
		return
	}

	if mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	}
	if blockProfileRate > 0 {
		runtime.SetBlockProfileRate(blockProfileRate)
	}

	util_log.InitLogger(&cfg.Server)

	// Allocate a block of memory to alter GC behaviour. See https://github.com/golang/go/issues/23044
	ballast := make([]byte, ballastBytes)

	util.InitEvents(eventSampleRate)

	// In testing mode skip JAEGER setup to avoid panic due to
	// "duplicate metrics collector registration attempted"
	if !testMode {
		name := "cortex"
		if len(cfg.Target) == 1 {
			name += "-" + cfg.Target[0]
		}

		// Setting the environment variable JAEGER_AGENT_HOST enables tracing.
		if trace, err := tracing.NewFromEnv(name); err != nil {
			level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
		} else {
			defer trace.Close()
		}
	}

	// Initialise seed for randomness usage.
	rand.Seed(time.Now().UnixNano())

	t, err := cortex.New(cfg)
	util_log.CheckFatal("initializing cortex", err)

	if printModules {
		allDeps := t.ModuleManager.DependenciesForModule(cortex.All)

		for _, m := range t.ModuleManager.UserVisibleModuleNames() {
			ix := sort.SearchStrings(allDeps, m)
			included := ix < len(allDeps) && allDeps[ix] == m

			if included {
				fmt.Fprintln(os.Stdout, m, "*")
			} else {
				fmt.Fprintln(os.Stdout, m)
			}
		}

		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, "Modules marked with * are included in target All.")
		return
	}

	level.Info(util_log.Logger).Log("msg", "Starting Cortex", "version", version.Info())

	err = t.Run()

	runtime.KeepAlive(ballast)
	util_log.CheckFatal("running cortex", err)
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

	// create a sha256 hash of the config before expansion and expose it via
	// the config_info metric
	hash := sha256.Sum256(buf)
	configHash.Reset()
	configHash.WithLabelValues(fmt.Sprintf("%x", hash)).Set(1)

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
