package main

import (
	"net/url"
	"strings"
)

// BackendConfig is a single backend config.
type BackendConfig struct {
	name     string
	endpoint url.URL
}

// String returns the backend endpoint.
func (b BackendConfig) String() string {
	return b.endpoint.String()
}

// BackendsConfig is a list of query backends.
type BackendsConfig []BackendConfig

// String implements flag.Value
func (bs BackendsConfig) String() string {
	output := strings.Builder{}

	for i, b := range bs {
		if i > 0 {
			output.WriteString(" ")
		}
		output.WriteString(b.String())
	}

	return output.String()
}

// Set implements flag.Value
func (bs *BackendsConfig) Set(s string) error {
	// Parse the URL.
	u, err := url.Parse(s)
	if err != nil {
		return err
	}

	*bs = append(*bs, BackendConfig{
		name:     u.Hostname(),
		endpoint: *u,
	})

	return nil
}
