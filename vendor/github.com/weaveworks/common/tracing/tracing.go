package tracing

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// New registers Jaeger as the OpenTracing implementation.
// If jaegerAgentHost is an empty string, tracing is disabled.
func New(agentHost, serviceName, samplerType string, samplerParam float64) io.Closer {
	if agentHost != "" {
		cfg := jaegercfg.Configuration{
			Sampler: &jaegercfg.SamplerConfig{
				SamplingServerURL: fmt.Sprintf("http://%s:5778/sampling", agentHost),
				Type:              samplerType,
				Param:             samplerParam,
			},
			Reporter: &jaegercfg.ReporterConfig{
				LocalAgentHostPort: fmt.Sprintf("%s:6831", agentHost),
			},
		}

		closer, err := cfg.InitGlobalTracer(serviceName)
		if err != nil {
			fmt.Printf("Could not initialize jaeger tracer: %s\n", err.Error())
			os.Exit(1)
		}
		return closer
	}
	return ioutil.NopCloser(nil)
}

// NewFromEnv is a convenience function to allow tracing configuration
// via environment variables
func NewFromEnv(serviceName string) io.Closer {
	agentHost := os.Getenv("JAEGER_AGENT_HOST")
	samplerType := os.Getenv("JAEGER_SAMPLER_TYPE")
	samplerParam, _ := strconv.ParseFloat(os.Getenv("JAEGER_SAMPLER_PARAM"), 64)
	if samplerType == "" || samplerParam == 0 {
		samplerType = "ratelimiting"
		samplerParam = 10.0
	}

	return New(agentHost, serviceName, samplerType, samplerParam)
}
