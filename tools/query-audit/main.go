package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "f", "", "path to config file")
	flag.Parse()
	if configFile == "" {
		log.Fatal(`unset configFile. try "-f <file>"`)
	}
}

func main() {
	var conf Config
	if err := LoadConfig(configFile, &conf); err != nil {
		log.Fatal(err)
	}

	err := Run(conf)
	if err != nil {
		log.Fatal(err)
	}
}

func Run(conf Config) error {
	ctlAPI, err := NewAPI(conf.Control)
	if err != nil {
		return err
	}

	tstAPI, err := NewAPI(conf.Test)
	if err != nil {
		return err
	}

	for _, query := range conf.Queries {
		ctlResp, _, err := ctlAPI.QueryRange(context.Background(), query.Query, v1.Range{
			Start: query.Start,
			End:   query.End,
			Step:  query.StepSize,
		})

		if err != nil {
			return err
		}

		tstResp, _, err := tstAPI.QueryRange(context.Background(), query.Query, v1.Range{
			Start: query.Start,
			End:   query.End,
			Step:  query.StepSize,
		})

		if err != nil {
			return err
		}

		auditor := &Auditor{}
		diff, err := auditor.Audit(ctlResp, tstResp)
		if err != nil {
			return err
		}

		fmt.Printf(
			"\n%f%% avg diff for:\n\tquery: %s\n\tseries: %d\n\tsamples: %d\n\tstart: %v\n\tend: %v\n\tstep: %v\n",
			diff.Diff*100,
			query.Query,
			diff.Series,
			len(diff.sampleDiffs),
			query.Start,
			query.End,
			query.StepSize,
		)

	}
	return nil
}
