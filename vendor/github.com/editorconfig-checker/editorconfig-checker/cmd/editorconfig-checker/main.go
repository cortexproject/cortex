// Package main provides ...
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/editorconfig-checker/editorconfig-checker/pkg/config"
	"github.com/editorconfig-checker/editorconfig-checker/pkg/error"
	"github.com/editorconfig-checker/editorconfig-checker/pkg/files"
	"github.com/editorconfig-checker/editorconfig-checker/pkg/logger"
	"github.com/editorconfig-checker/editorconfig-checker/pkg/validation"
)

// version is used vor the help
const version string = "2.0.2"

// defaultConfigFilePath determines where the config is located
const defaultConfigFilePath = ".ecrc"

// currentConfig is the config used in this run
var currentConfig *config.Config

// Init function, runs on start automagically
func init() {
	var configFilePath string
	var tmpExclude string
	var c config.Config
	var init bool

	flag.BoolVar(&init, "init", false, "creates an initial configuration")
	flag.StringVar(&configFilePath, "config", "", "config")
	flag.StringVar(&tmpExclude, "exclude", "", "a regex which files should be excluded from checking - needs to be a valid regular expression")
	flag.BoolVar(&c.IgnoreDefaults, "ignore-defaults", false, "ignore default excludes")
	flag.BoolVar(&c.DryRun, "dry-run", false, "show which files would be checked")
	flag.BoolVar(&c.Version, "version", false, "print the version number")
	flag.BoolVar(&c.Help, "help", false, "print the help")
	flag.BoolVar(&c.Help, "h", false, "print the help")
	flag.BoolVar(&c.Verbose, "verbose", false, "print debugging information")
	flag.BoolVar(&c.Verbose, "v", false, "print debugging information")
	flag.BoolVar(&c.Debug, "debug", false, "print debugging information")
	flag.BoolVar(&c.NoColor, "no-color", false, "dont print colors")
	flag.BoolVar(&c.Disable.TrimTrailingWhitespace, "disable-trim-trailing-whitespace", false, "disables the trailing whitespace check")
	flag.BoolVar(&c.Disable.EndOfLine, "disable-end-of-line", false, "disables the trailing whitespace check")
	flag.BoolVar(&c.Disable.InsertFinalNewline, "disable-insert-final-newline", false, "disables the final newline check")
	flag.BoolVar(&c.Disable.Indentation, "disable-indentation", false, "disables the indentation check")

	flag.Parse()

	if configFilePath == "" {
		configFilePath = defaultConfigFilePath
	}

	currentConfig, _ = config.NewConfig(configFilePath)

	if init {
		err := currentConfig.Save()
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}

		os.Exit(0)
	}

	_ = currentConfig.Parse()
	if tmpExclude != "" {
		c.Exclude = append(c.Exclude, tmpExclude)
	}

	c.PassedFiles = flag.Args()
	c.Logger = logger.Logger{Verbosee: c.Verbose, Debugg: c.Debug, NoColor: c.NoColor}

	currentConfig.Merge(c)
}

// Main function, dude
func main() {
	config := *currentConfig
	currentConfig.Logger.Debug(currentConfig.GetAsString())
	currentConfig.Logger.Verbose("Exclude Regexp: %s", currentConfig.GetExcludesAsRegularExpression())

	// Check for returnworthy params
	shouldExit := ReturnableFlags(config)
	if shouldExit {
		os.Exit(0)
	}

	// contains all files which should be checked
	filePaths := files.GetFiles(config)

	if currentConfig.DryRun {
		for _, file := range filePaths {
			currentConfig.Logger.Output(file)
		}

		os.Exit(0)
	}

	errors := validation.ProcessValidation(filePaths, config)
	errorCount := error.GetErrorCount(errors)

	if errorCount != 0 {
		error.PrintErrors(errors, config)
		logger.Error(fmt.Sprintf("\n%d errors found", errorCount))
	}

	currentConfig.Logger.Verbose("%d files checked", len(filePaths))

	if errorCount != 0 {
		os.Exit(1)
	}

	os.Exit(0)
}

// ReturnableFlags returns wether a flag passed should exit the program
func ReturnableFlags(config config.Config) bool {
	switch {
	case config.Version:
		config.Logger.Output(version)
	case config.Help:
		config.Logger.Output("USAGE:")
		flag.PrintDefaults()
	}

	return config.Version || config.Help
}
