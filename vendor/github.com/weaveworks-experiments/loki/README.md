# Loki: Simple Distributed Tracing

Loki is a distributed tracing system inspired by Zipkin and Prometheus.

- Pull Based: a central Loki app pull traces from your instrumented applications
- Service Discover: using Prometheus' Service Discovery frameworks allows Loki to discover your app within many popular orchestrators (Kubernetes, Mesos etc) or service discovery systems (Consul, DNS etc)

Loki consists of:
- A OpenTracing compatible tracer
- The Loki app

Internally Loki is really just an opinionated reimplementation of OpenZipkin.

## Instrumenting your app

Instrument you go application according to OpenTracing
- For gRPC, use https://github.com/grpc-ecosystem/grpc-opentracing/tree/master/go/otgrpc
- For HTTP, use https://github.com/opentracing-contrib/go-stdlib

```go
import (
    "github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
    "github.com/weaveworks-experiments/pkg/loki/client"
)

func main() {
    // Create a Loki tracer
    tracer, err := loki.NewTracer(loki.DefaultConfig)

  	// explicitly set our tracer to be the default tracer.
  	opentracing.InitGlobalTracer(tracer)

    // Create an instrumented gRPC server
    s := grpc.NewServer(
        grpc.UnaryInterceptor(
            otgrpc.OpenTracingServerInterceptor(tracer),
        ),
    )

    // Register a http handler for Loki
    http.Handle("/traces", loki.Handler())
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```
