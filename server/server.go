package server

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // anonymous import to get the pprof handler registered

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"

	"github.com/weaveworks-experiments/loki/pkg/client"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/signals"
	"github.com/weaveworks/cortex/ring"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
)

func init() {
	prometheus.MustRegister(requestDuration)
}

// Config for a Server
type Config struct {
	LogSuccess     bool
	HTTPListenPort int
	GRPCListenPort int
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.LogSuccess, "server.log-success", false, "Log successful requests")
	f.IntVar(&cfg.HTTPListenPort, "server.http-listen-port", 9094, "HTTP server listen port.")
	f.IntVar(&cfg.GRPCListenPort, "server.grpc-listen-port", 9095, "gRPC server listen port.")
}

// Server wraps a HTTP and gRPC server, and some common initialization.
type Server struct {
	cfg Config

	HTTP *mux.Router
	GRPC *grpc.Server
}

func init() {
	tracer, err := loki.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("Failed to create tracer: %v", err))
	} else {
		opentracing.InitGlobalTracer(tracer)
	}
}

// New makes a new Server
func New(cfg Config, r *ring.Ring) *Server {
	router := mux.NewRouter()
	if r != nil {
		router.Handle("/ring", r)
	}
	router.Handle("/metrics", prometheus.Handler())
	router.PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)

	router.Handle("/traces", loki.Handler())

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			middleware.ServerLoggingInterceptor(cfg.LogSuccess),
			middleware.ServerInstrumentInterceptor(requestDuration),
			otgrpc.OpenTracingServerInterceptor(opentracing.GlobalTracer()),
			middleware.ServerUserHeaderInterceptor,
		)),
	)

	return &Server{
		cfg:  cfg,
		HTTP: router,
		GRPC: grpcServer,
	}
}

// Run the server; blocks until SIGTERM is received.
func (s *Server) Run() {
	// Setup HTTP server
	intermediate := middleware.Merge(
		middleware.Log{
			LogSuccess: s.cfg.LogSuccess,
		},
		middleware.Instrument{
			Duration:     requestDuration,
			RouteMatcher: s.HTTP,
		},
	).Wrap(s.HTTP)
	// for HTTP over gRPC, ensure we don't double-count the tracing context
	httpgrpc.RegisterHTTPServer(s.GRPC, httpgrpc.NewServer(intermediate))
	go http.ListenAndServe(
		fmt.Sprintf(":%d", s.cfg.HTTPListenPort),
		middleware.Func(func(handler http.Handler) http.Handler {
			return nethttp.Middleware(opentracing.GlobalTracer(), handler)
		}).Wrap(intermediate),
	)

	// Setup gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.GRPCListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go s.GRPC.Serve(lis)

	signals.SignalHandlerLoop(log.Base())
}

// Stop the server.  Does not unblock Run!
func (s *Server) Stop() {
	s.GRPC.Stop()
}
