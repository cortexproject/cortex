package main

import (
	"net/http"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewAPI instantiates a prometheus api
func NewAPI(backend Backend) (v1.API, error) {
	config := api.Config{
		Address: backend.Host,
	}

	if len(backend.Headers) > 0 {
		config.RoundTripper = promhttp.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			for key, value := range backend.Headers {
				req.Header.Add(key, value)
			}
			return http.DefaultTransport.RoundTrip(req)
		})
	}

	c, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}

	return v1.NewAPI(c), nil
}
