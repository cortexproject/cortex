// Package config contains functions and structs related to config
package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/editorconfig-checker/editorconfig-checker/pkg/logger"
	"github.com/editorconfig-checker/editorconfig-checker/pkg/utils"
)

// DefaultExcludes is the regular expression for ignored files
var DefaultExcludes = strings.Join(defaultExcludes, "|")

// defaultExcludes are an array to produce the correct string from
var defaultExcludes = []string{
	"yarn\\.lock$",
	"package-lock\\.json",
	"composer\\.lock$",
	"\\.snap$",
	"\\.otf$",
	"\\.woff$",
	"\\.woff2$",
	"\\.eot$",
	"\\.ttf$",
	"\\.gif$",
	"\\.png$",
	"\\.jpg$",
	"\\.jpeg$",
	"\\.mp4$",
	"\\.wmv$",
	"\\.svg$",
	"\\.ico$",
	"\\.bak$",
	"\\.bin$",
	"\\.pdf$",
	"\\.zip$",
	"\\.gz$",
	"\\.tar$",
	"\\.7z$",
	"\\.bz2$",
	"\\.log$",
	"\\.css\\.map$",
	"\\.js\\.map$",
	"min\\.css$",
	"min\\.js$",
}

var defaultAllowedContentTypes = []string{
	"text/",
	"application/octet-stream",
}

// Config struct, contains everything a config can contain
type Config struct {
	// CLI
	Version bool
	Help    bool
	DryRun  bool
	Path    string

	// CONFIG FILE
	Verbose             bool
	Debug               bool
	IgnoreDefaults      bool
	SpacesAftertabs     bool
	NoColor             bool
	Exclude             []string
	AllowedContentTypes []string
	PassedFiles         []string
	Disable             DisabledChecks

	// MISC
	Logger logger.Logger
}

// DisabledChecks is a Struct which represents disabled checks
type DisabledChecks struct {
	EndOfLine              bool
	Indentation            bool
	InsertFinalNewline     bool
	TrimTrailingWhitespace bool
}

// NewConfig initializes a new config
func NewConfig(configPath string) (*Config, error) {
	var config Config

	config.Path = configPath
	config.AllowedContentTypes = defaultAllowedContentTypes
	config.Exclude = []string{}
	config.PassedFiles = []string{}

	if !utils.IsRegularFile(configPath) {
		return &config, fmt.Errorf("No file found at %s", configPath)
	}

	return &config, nil
}

// Parse parses a config at a given path
func (c *Config) Parse() error {
	if c.Path != "" {
		configString, err := ioutil.ReadFile(c.Path)
		if err != nil {
			return err
		}

		tmpConfg := Config{}
		err = json.Unmarshal(configString, &tmpConfg)
		if err != nil {
			return err
		}

		c.Merge(tmpConfg)
	}

	return nil
}

// Merge merges a provided config with a config
func (c *Config) Merge(config Config) {
	if config.DryRun {
		c.DryRun = config.DryRun
	}

	if config.Version {
		c.Version = config.Version
	}

	if config.Help {
		c.Help = config.Help
	}

	if config.Verbose {
		c.Verbose = config.Verbose
	}

	if config.Debug {
		c.Debug = config.Debug
	}

	if config.IgnoreDefaults {
		c.IgnoreDefaults = config.IgnoreDefaults
	}

	if config.SpacesAftertabs {
		c.SpacesAftertabs = config.SpacesAftertabs
	}

	if config.Path != "" {
		c.Path = config.Path
	}

	if len(config.Exclude) != 0 {
		c.Exclude = append(c.Exclude, config.Exclude...)
	}

	if len(config.AllowedContentTypes) != 0 {
		c.AllowedContentTypes = append(c.AllowedContentTypes, config.AllowedContentTypes...)
	}

	if len(config.PassedFiles) != 0 {
		c.PassedFiles = config.PassedFiles
	}

	c.mergeDisabled(config.Disable)
	c.Logger = config.Logger
}

// mergeDisabled merges the disabled checks into the config
// This is here because cyclomatic complexity of gocyclo was about 15 :/
func (c *Config) mergeDisabled(disabled DisabledChecks) {
	if disabled.EndOfLine {
		c.Disable.EndOfLine = disabled.EndOfLine
	}

	if disabled.TrimTrailingWhitespace {
		c.Disable.TrimTrailingWhitespace = disabled.TrimTrailingWhitespace
	}

	if disabled.InsertFinalNewline {
		c.Disable.InsertFinalNewline = disabled.InsertFinalNewline
	}

	if disabled.Indentation {
		c.Disable.Indentation = disabled.Indentation
	}
}

// GetExcludesAsRegularExpression returns the excludes as a combined regular expression
func (c Config) GetExcludesAsRegularExpression() string {
	if c.IgnoreDefaults {
		return strings.Join(c.Exclude, "|")
	}

	return strings.Join(append(c.Exclude, DefaultExcludes), "|")
}

// Save saves the config to it's Path
func (c Config) Save() error {
	if utils.IsRegularFile(c.Path) {
		return fmt.Errorf("File `%v` already exists", c.Path)
	}

	type writtenConfig struct {
		Verbose             bool
		Debug               bool
		IgnoreDefaults      bool
		SpacesAftertabs     bool
		NoColor             bool
		Exclude             []string
		AllowedContentTypes []string
		PassedFiles         []string
		Disable             DisabledChecks
	}

	configJSON, _ := json.MarshalIndent(writtenConfig{}, "", "  ")
	configString := strings.Replace(string(configJSON[:]), "null", "[]", -1)
	err := ioutil.WriteFile(c.Path, []byte(configString), 0644)

	return err
}

// GetAsString returns the config in a readable form
func (c Config) GetAsString() string {
	return fmt.Sprintf("Config: %+v", c)
}
