package main

import (
	"io/ioutil"
	"time"

	"github.com/pkg/errors"

	"sigs.k8s.io/yaml"
)

type Backend struct {
	Host    string            `yaml:"host" json:"host"`
	Headers map[string]string `yaml:"headers" json:"headers"`
}

type Query struct {
	Query       string    `yaml:"query" json:"query"`
	Start       time.Time `yaml:"start" json:"start"`
	End         time.Time `yaml:"end" json:"end"`
	StepSizeStr string    `yaml:"step_size" json:"step_size"`
	StepSize    time.Duration
}

func (q *Query) Validate() error {
	parsedDur, err := time.ParseDuration(q.StepSizeStr)
	if err != nil {
		return err
	}

	q.StepSize = parsedDur

	if q.StepSize == time.Duration(0) {
		q.StepSize = time.Minute
	}
	return nil
}

type Config struct {
	Control Backend  `yaml:"control" json:"control"`
	Test    Backend  `yaml:"test" json:"test"`
	Queries []*Query `yaml:"queries" json:"queries"`
}

func (cfg *Config) Validate() error {
	for _, q := range cfg.Queries {
		if err := q.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// LoadConfig read YAML-formatted config from filename into cfg.
func LoadConfig(filename string, cfg *Config) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	err = yaml.Unmarshal(buf, cfg)
	if err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return cfg.Validate()
}
