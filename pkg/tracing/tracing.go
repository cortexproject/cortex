package tracing

import (
	"fmt"
	"io"
	"os"

	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

type nopCloser struct {
}

func (nopCloser) Close() error { return nil }

// New registers Jaeger as the OpenTracing implementation.
func New(serviceName string) io.Closer {
	jaegerAgentHost := os.Getenv("JAEGER_AGENT_HOST")
	if jaegerAgentHost != "" {
		cfg := jaegercfg.Configuration{
			Sampler: &jaegercfg.SamplerConfig{
				SamplingServerURL: fmt.Sprintf("http://%s:5778/sampling", jaegerAgentHost),
				Type:              jaeger.SamplerTypeConst,
				Param:             1,
			},
			Reporter: &jaegercfg.ReporterConfig{
				LocalAgentHostPort: fmt.Sprintf("%s:6831", jaegerAgentHost),
			},
		}

		closer, err := cfg.InitGlobalTracer(serviceName)
		if err != nil {
			fmt.Printf("Could not initialize jaeger tracer: %s\n", err.Error())
			os.Exit(1)
		}
		return closer
	}
	return nopCloser{}
}
