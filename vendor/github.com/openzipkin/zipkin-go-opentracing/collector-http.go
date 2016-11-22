package zipkintracer

import (
	"bytes"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

// Default timeout for http request in seconds
const defaultHTTPTimeout = time.Second * 5

// defaultBatchInterval in seconds
const defaultHTTPBatchInterval = 1

// HTTPCollector implements Collector by forwarding spans to a http server.
type HTTPCollector struct {
	logger        Logger
	url           string
	client        *http.Client
	nextSend      time.Time
	batchInterval time.Duration
	batchSize     int
	batch         []*zipkincore.Span
	spanc         chan *zipkincore.Span
	sendc         chan struct{}
	quit          chan struct{}
}

// HTTPOption sets a parameter for the HttpCollector
type HTTPOption func(c *HTTPCollector)

// HTTPLogger sets the logger used to report errors in the collection
// process. By default, a no-op logger is used, i.e. no errors are logged
// anywhere. It's important to set this option in a production service.
func HTTPLogger(logger Logger) HTTPOption {
	return func(c *HTTPCollector) { c.logger = logger }
}

// HTTPTimeout sets maximum timeout for http request.
func HTTPTimeout(duration time.Duration) HTTPOption {
	return func(c *HTTPCollector) { c.client.Timeout = duration }
}

// HTTPBatchSize sets the maximum batch size, after which a collect will be
// triggered. The default batch size is 100 traces.
func HTTPBatchSize(n int) HTTPOption {
	return func(c *HTTPCollector) { c.batchSize = n }
}

// HTTPBatchInterval sets the maximum duration we will buffer traces before
// emitting them to the collector. The default batch interval is 1 second.
func HTTPBatchInterval(d time.Duration) HTTPOption {
	return func(c *HTTPCollector) { c.batchInterval = d }
}

// NewHTTPCollector returns a new HTTP-backend Collector. url should be a http
// url for handle post request. timeout is passed to http client. queueSize control
// the maximum size of buffer of async queue. The logger is used to log errors,
// such as send failures;
func NewHTTPCollector(url string, options ...HTTPOption) (Collector, error) {
	c := &HTTPCollector{
		logger:        NewNopLogger(),
		url:           url,
		client:        &http.Client{Timeout: defaultHTTPTimeout},
		batchInterval: defaultHTTPBatchInterval * time.Second,
		batchSize:     100,
		batch:         []*zipkincore.Span{},
		spanc:         make(chan *zipkincore.Span),
		sendc:         make(chan struct{}),
		quit:          make(chan struct{}, 1),
	}

	for _, option := range options {
		option(c)
	}
	c.nextSend = time.Now().Add(c.batchInterval)
	go c.loop()
	return c, nil
}

// Collect implements Collector.
func (c *HTTPCollector) Collect(s *zipkincore.Span) error {
	c.spanc <- s
	return nil
}

// Close implements Collector.
func (c *HTTPCollector) Close() error {
	c.quit <- struct{}{}
	return nil
}

func httpSerialize(spans []*zipkincore.Span) *bytes.Buffer {
	t := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(t)
	if err := p.WriteListBegin(thrift.STRUCT, len(spans)); err != nil {
		panic(err)
	}
	for _, s := range spans {
		if err := s.Write(p); err != nil {
			panic(err)
		}
	}
	if err := p.WriteListEnd(); err != nil {
		panic(err)
	}
	return t.Buffer
}

func (c *HTTPCollector) loop() {
	tickc := time.Tick(c.batchInterval / 10)

	for {
		select {
		case span := <-c.spanc:
			c.batch = append(c.batch, span)
			if len(c.batch) >= c.batchSize {
				go c.sendNow()
			}
		case <-tickc:
			if time.Now().After(c.nextSend) {
				go c.sendNow()
			}
		case <-c.sendc:
			c.nextSend = time.Now().Add(c.batchInterval)
			if err := c.send(c.batch); err != nil {
				_ = c.logger.Log("err", err.Error())
			}
			c.batch = c.batch[:0]
		case <-c.quit:
			return
		}
	}
}

func (c *HTTPCollector) send(spans []*zipkincore.Span) error {
	req, err := http.NewRequest(
		"POST",
		c.url,
		httpSerialize(spans))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-thrift")

	_, err = c.client.Do(req)

	return err
}

func (c *HTTPCollector) sendNow() {
	c.sendc <- struct{}{}
}
