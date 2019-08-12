// Copyright 2015 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ui

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" // Comment this line to disable pprof endpoint.
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
)

func serveAsset(w http.ResponseWriter, req *http.Request, fp string, logger log.Logger) {
	info, err := AssetInfo(fp)
	if err != nil {
		level.Warn(logger).Log("msg", "Could not get file", "err", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	file, err := Asset(fp)
	if err != nil {
		if err != io.EOF {
			level.Warn(logger).Log("msg", "Could not get file", "file", fp, "err", err)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
}

// Register registers handlers to serve files for the web interface.
func Register(r *route.Router, reloadCh chan<- chan error, logger log.Logger) {
	ihf := InstrumentHandlerFunc

	r.Get("/metrics", promhttp.Handler().ServeHTTP)

	r.Get("/", ihf("index", func(w http.ResponseWriter, req *http.Request) {
		serveAsset(w, req, "ui/app/index.html", logger)
	}))

	r.Get("/script.js", ihf("app", func(w http.ResponseWriter, req *http.Request) {
		serveAsset(w, req, "ui/app/script.js", logger)
	}))

	r.Get("/favicon.ico", ihf("app", func(w http.ResponseWriter, req *http.Request) {
		serveAsset(w, req, "ui/app/favicon.ico", logger)
	}))

	r.Get("/lib/*filepath", ihf("lib_files",
		func(w http.ResponseWriter, req *http.Request) {
			fp := route.Param(req.Context(), "filepath")
			serveAsset(w, req, filepath.Join("ui/app/lib", fp), logger)
		},
	))

	r.Post("/-/reload", func(w http.ResponseWriter, req *http.Request) {
		errc := make(chan error)
		defer close(errc)

		reloadCh <- errc
		if err := <-errc; err != nil {
			http.Error(w, fmt.Sprintf("failed to reload config: %s", err), http.StatusInternalServerError)
		}
	})

	r.Get("/-/healthy", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})

	r.Get("/debug/*subpath", http.DefaultServeMux.ServeHTTP)
	r.Post("/debug/*subpath", http.DefaultServeMux.ServeHTTP)
}

// InstrumentHandlerFunc wraps the given function for instrumentation. It
// otherwise works in the same way as InstrumentHandler (and shares the same
// issues).
//
// Deprecated: InstrumentHandlerFunc has several issues:
//
// - It uses Summaries rather than Histograms. Summaries are not useful if
// aggregation across multiple instances is required.
//
// - It uses microseconds as unit, which is deprecated and should be replaced by
// seconds.
//
// - The size of the request is calculated in a separate goroutine. Since this
// calculator requires access to the request header, it creates a race with
// any writes to the header performed during request handling.
// httputil.ReverseProxy is a prominent example for a handler
// performing such writes.
//
// Upcoming versions of this package will provide ways of instrumenting HTTP
// handlers that are more flexible and have fewer issues. Please prefer direct
// instrumentation in the meantime.
func InstrumentHandlerFunc(handlerName string, handlerFunc func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return InstrumentHandlerFuncWithOpts(
		prometheus.SummaryOpts{
			Subsystem:   "http",
			ConstLabels: prometheus.Labels{"handler": handlerName},
		},
		handlerFunc,
	)
}

// InstrumentHandlerFuncWithOpts works like InstrumentHandlerFunc (and shares
// the same issues) but provides more flexibility (at the cost of a more complex
// call syntax). See InstrumentHandlerWithOpts for details how the provided
// SummaryOpts are used.
//
// Deprecated: InstrumentHandlerFuncWithOpts is deprecated for the same reasons
// as InstrumentHandlerFunc is.
func InstrumentHandlerFuncWithOpts(opts prometheus.SummaryOpts, handlerFunc func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	reqCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   opts.Namespace,
			Subsystem:   opts.Subsystem,
			Name:        "requests_total",
			Help:        "Total number of HTTP requests made.",
			ConstLabels: opts.ConstLabels,
		},
		instLabels,
	)

	opts.Name = "request_duration_microseconds"
	opts.Help = "The HTTP request latencies in microseconds."
	reqDur := prometheus.NewSummary(opts)

	opts.Name = "request_size_bytes"
	opts.Help = "The HTTP request sizes in bytes."
	reqSz := prometheus.NewSummary(opts)

	opts.Name = "response_size_bytes"
	opts.Help = "The HTTP response sizes in bytes."
	resSz := prometheus.NewSummary(opts)

	regReqCnt := MustRegisterOrGet(reqCnt).(*prometheus.CounterVec)
	regReqDur := MustRegisterOrGet(reqDur).(prometheus.Summary)
	regReqSz := MustRegisterOrGet(reqSz).(prometheus.Summary)
	regResSz := MustRegisterOrGet(resSz).(prometheus.Summary)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()

		delegate := &responseWriterDelegator{ResponseWriter: w}
		out := make(chan int)
		urlLen := 0
		if r.URL != nil {
			urlLen = len(r.URL.String())
		}
		go computeApproximateRequestSize(r, out, urlLen)

		_, cn := w.(http.CloseNotifier)
		_, fl := w.(http.Flusher)
		_, hj := w.(http.Hijacker)
		_, rf := w.(io.ReaderFrom)
		var rw http.ResponseWriter
		if cn && fl && hj && rf {
			rw = &fancyResponseWriterDelegator{delegate}
		} else {
			rw = delegate
		}
		handlerFunc(rw, r)

		elapsed := float64(time.Since(now)) / float64(time.Microsecond)

		method := sanitizeMethod(r.Method)
		code := sanitizeCode(delegate.status)
		regReqCnt.WithLabelValues(method, code).Inc()
		regReqDur.Observe(elapsed)
		regResSz.Observe(float64(delegate.written))
		regReqSz.Observe(float64(<-out))
	})
}

func computeApproximateRequestSize(r *http.Request, out chan int, s int) {
	s += len(r.Method)
	s += len(r.Proto)
	for name, values := range r.Header {
		s += len(name)
		for _, value := range values {
			s += len(value)
		}
	}
	s += len(r.Host)

	// N.B. r.Form and r.MultipartForm are assumed to be included in r.URL.

	if r.ContentLength != -1 {
		s += int(r.ContentLength)
	}
	out <- s
}

func sanitizeMethod(m string) string {
	switch m {
	case "GET", "get":
		return "get"
	case "PUT", "put":
		return "put"
	case "HEAD", "head":
		return "head"
	case "POST", "post":
		return "post"
	case "DELETE", "delete":
		return "delete"
	case "CONNECT", "connect":
		return "connect"
	case "OPTIONS", "options":
		return "options"
	case "NOTIFY", "notify":
		return "notify"
	default:
		return strings.ToLower(m)
	}
}

var instLabels = []string{"method", "code"}

type responseWriterDelegator struct {
	http.ResponseWriter

	handler, method string
	status          int
	written         int64
	wroteHeader     bool
}

func (r *responseWriterDelegator) WriteHeader(code int) {
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseWriterDelegator) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.written += int64(n)
	return n, err
}

type fancyResponseWriterDelegator struct {
	*responseWriterDelegator
}

func (f *fancyResponseWriterDelegator) CloseNotify() <-chan bool {
	return f.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (f *fancyResponseWriterDelegator) Flush() {
	f.ResponseWriter.(http.Flusher).Flush()
}

func (f *fancyResponseWriterDelegator) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return f.ResponseWriter.(http.Hijacker).Hijack()
}

func (f *fancyResponseWriterDelegator) ReadFrom(r io.Reader) (int64, error) {
	if !f.wroteHeader {
		f.WriteHeader(http.StatusOK)
	}
	n, err := f.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	f.written += n
	return n, err
}

func sanitizeCode(s int) string {
	switch s {
	case 100:
		return "100"
	case 101:
		return "101"

	case 200:
		return "200"
	case 201:
		return "201"
	case 202:
		return "202"
	case 203:
		return "203"
	case 204:
		return "204"
	case 205:
		return "205"
	case 206:
		return "206"

	case 300:
		return "300"
	case 301:
		return "301"
	case 302:
		return "302"
	case 304:
		return "304"
	case 305:
		return "305"
	case 307:
		return "307"

	case 400:
		return "400"
	case 401:
		return "401"
	case 402:
		return "402"
	case 403:
		return "403"
	case 404:
		return "404"
	case 405:
		return "405"
	case 406:
		return "406"
	case 407:
		return "407"
	case 408:
		return "408"
	case 409:
		return "409"
	case 410:
		return "410"
	case 411:
		return "411"
	case 412:
		return "412"
	case 413:
		return "413"
	case 414:
		return "414"
	case 415:
		return "415"
	case 416:
		return "416"
	case 417:
		return "417"
	case 418:
		return "418"

	case 500:
		return "500"
	case 501:
		return "501"
	case 502:
		return "502"
	case 503:
		return "503"
	case 504:
		return "504"
	case 505:
		return "505"

	case 428:
		return "428"
	case 429:
		return "429"
	case 431:
		return "431"
	case 511:
		return "511"

	default:
		return strconv.Itoa(s)
	}
}

// RegisterOrGet registers the provided Collector with the DefaultRegisterer and
// returns the Collector, unless an equal Collector was registered before, in
// which case that Collector is returned.
func RegisterOrGet(c prometheus.Collector) (prometheus.Collector, error) {
	if err := prometheus.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector, nil
		}
		return nil, err
	}
	return c, nil
}

// MustRegisterOrGet behaves like RegisterOrGet but panics instead of returning
// an error.
func MustRegisterOrGet(c prometheus.Collector) prometheus.Collector {
	c, err := RegisterOrGet(c)
	if err != nil {
		panic(err)
	}
	return c
}
