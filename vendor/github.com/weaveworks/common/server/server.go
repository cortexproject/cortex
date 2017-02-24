package server

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // anonymous import to get the pprof handler registered

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/weaveworks-experiments/loki/pkg/client"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/signals"
)

func init() {
	tracer, err := loki.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("Failed to create tracer: %v", err))
	} else {
		opentracing.InitGlobalTracer(tracer)
	}
}

// Config for a Server
type Config struct {
	MetricsNamespace string
	LogSuccess       bool
	HTTPListenPort   int
	GRPCListenPort   int
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.LogSuccess, "server.log-success", false, "Log successful requests")
	f.IntVar(&cfg.HTTPListenPort, "server.http-listen-port", 80, "HTTP server listen port.")
	f.IntVar(&cfg.GRPCListenPort, "server.grpc-listen-port", 9095, "gRPC server listen port.")
}

// Server wraps a HTTP and gRPC server, and some common initialization.
//
// Servers will be automatically instrumented for Prometheus metrics
// and Loki tracing.  HTTP over gRPC
type Server struct {
	cfg             Config
	requestDuration *prometheus.HistogramVec

	HTTP *mux.Router
	GRPC *grpc.Server
}

// New makes a new Server
func New(cfg Config) *Server {
	requestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: cfg.MetricsNamespace,
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
	prometheus.MustRegister(requestDuration)

	router := mux.NewRouter()
	router.Handle("/metrics", prometheus.Handler())
	router.Handle("/traces", loki.Handler())
	router.PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			middleware.ServerLoggingInterceptor(cfg.LogSuccess),
			middleware.ServerInstrumentInterceptor(requestDuration),
			otgrpc.OpenTracingServerInterceptor(opentracing.GlobalTracer()),
			middleware.ServerUserHeaderInterceptor,
		)),
	)

	return &Server{
		cfg:             cfg,
		requestDuration: requestDuration,
		HTTP:            router,
		GRPC:            grpcServer,
	}
}

// Run the server; blocks until SIGTERM is received.
func (s *Server) Run() {
	// Setup HTTP server
	go http.ListenAndServe(
		fmt.Sprintf(":%d", s.cfg.HTTPListenPort),
		middleware.Merge(
			middleware.Log{
				LogSuccess: s.cfg.LogSuccess,
			},
			middleware.Instrument{
				Duration:     s.requestDuration,
				RouteMatcher: s.HTTP,
			},
			middleware.Func(func(handler http.Handler) http.Handler {
				return nethttp.Middleware(opentracing.GlobalTracer(), handler)
			}),
		).Wrap(s.HTTP),
	)

	// Setup gRPC server
	// for HTTP over gRPC, ensure we don't double-count the middleware
	httpgrpc.RegisterHTTPServer(s.GRPC, httpgrpc.NewServer(s.HTTP))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.GRPCListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go s.GRPC.Serve(lis)

	signals.SignalHandlerLoop(log.StandardLogger())
}

// Stop the server.  Does not unblock Run!
func (s *Server) Stop() {
	s.GRPC.Stop()
}
