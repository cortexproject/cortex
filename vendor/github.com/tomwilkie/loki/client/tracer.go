package loki

import (
	"fmt"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing"
)

type Config struct {
	ServiceName string
}

var DefaultConfig = Config{}

func NewTracer(cfg Config) (opentracing.Tracer, error) {
	// create recorder.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	recorder := zipkintracer.NewRecorder(globalCollector, false, hostname, cfg.ServiceName)

	// create tracer.
	tracer, err := zipkintracer.NewTracer(recorder)
	if err != nil {
		fmt.Printf("unable to create Zipkin tracer: %+v", err)
		os.Exit(-1)
	}

	return tracer, nil
}
