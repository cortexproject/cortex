package loki

import (
	"github.com/opentracing/opentracing-go"
	"github.com/weaveworks-experiments/loki/pkg/model"
)

func NewTracer() (opentracing.Tracer, error) {
	return &model.Tracer{
		Collector: globalCollector,
	}, nil
}
