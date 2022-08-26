package tracing

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/tracing"
	"go.opentelemetry.io/otel/propagation"

	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"

	"github.com/cortexproject/cortex/pkg/tracing/migration"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const (
	JaegerType = "jaeger"
	OtelType   = "otel"
)

type Config struct {
	Type string `yaml:"type" json:"type"`
	Otel Otel   `yaml:"otel" json:"otel"`
}

type Otel struct {
	OltpEndpoint string  `yaml:"oltp_endpoint" json:"oltp_endpoint"`
	Insecure     bool    `yaml:"insecure" json:"insecure"`
	ExporterType string  `yaml:"exporter_type" json:"exporter_type"`
	SampleRatio  float64 `yaml:"sample_ratio" json:"sample_ratio"`
}

// RegisterFlags registers flag.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	p := "tracing"
	f.StringVar(&c.Type, p+".type", JaegerType, "Tracing type. OTEL and JAEGER are currently supported. For jaeger `JAEGER_AGENT_HOST` environment variable should also be set. See: https://cortexmetrics.io/docs/guides/tracing .")
	f.Float64Var(&c.Otel.SampleRatio, p+".otel.sample-ration", 0.001, "Fraction of traces to be sampled. Fractions >= 1 means sampling if off and everything is traced.")
	f.StringVar(&c.Otel.OltpEndpoint, p+".otel.oltp-endpoint", "", "otl collector endpoint that the driver will use to send spans.")
	f.BoolVar(&c.Otel.Insecure, p+".otel.insecure", false, "Disables client transport security for the exporter.")
	f.StringVar(&c.Otel.ExporterType, p+".otel.exporter-type", "", "enhance/modify traces/propagators for specific exporter. If empty, OTEL defaults will apply. Supported values are: `awsxray.`")
}

func (c *Config) Validate() error {
	switch strings.ToLower(c.Type) {
	case OtelType:
		if c.Otel.OltpEndpoint == "" {
			return errors.New("oltp-endpoint must be defined when using otel exporter")
		}
	}

	return nil
}

func SetupTracing(ctx context.Context, name string, c Config) (func(context.Context) error, error) {
	switch strings.ToLower(c.Type) {
	case JaegerType:
		// Setting the environment variable JAEGER_AGENT_HOST enables tracing.
		if trace, err := tracing.NewFromEnv(name); err != nil {
			level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
		} else {
			return func(ctx context.Context) error {
				trace.Close()
				return nil
			}, nil
		}
	case OtelType:
		util_log.Logger.Log("msg", "creating otel exporter")

		options := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(c.Otel.OltpEndpoint),
		}

		if c.Otel.Insecure {
			options = append(options, otlptracegrpc.WithInsecure())
		}

		exporter, err := otlptracegrpc.New(ctx, options...)
		if err != nil {
			return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
		}

		tracerProvider := newTraceProvider(name, c, exporter)

		bridge, wrappedProvider := migration.NewCortexBridgeTracerWrapper(tracerProvider.Tracer("github.com/cortexproject/cortex/cmd/cortex"))
		bridge.SetTextMapPropagator(propagation.TraceContext{})
		opentracing.SetGlobalTracer(bridge)
		otel.SetTracerProvider(wrappedProvider)

		return tracerProvider.Shutdown, nil
	}

	return func(ctx context.Context) error {
		return nil
	}, nil
}

func newTraceProvider(name string, c Config, exporter *otlptrace.Exporter) *sdktrace.TracerProvider {
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(c.Otel.SampleRatio))),
		sdktrace.WithResource(newResource(name)),
	}

	switch strings.ToLower(c.Otel.ExporterType) {
	case "awsxray":
		options = append(options, sdktrace.WithIDGenerator(xray.NewIDGenerator()))
	}

	return sdktrace.NewTracerProvider(options...)
}

func newResource(target string) *resource.Resource {
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(target),
	)
}
