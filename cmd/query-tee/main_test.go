package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCortexReadRoutes(t *testing.T) {
	routes := cortexReadRoutes(Config{PathPrefix: ""})
	for _, r := range routes {
		assert.True(t, strings.HasPrefix(r.Path, "/api/v1/"))
	}

	routes = cortexReadRoutes(Config{PathPrefix: "/some/random/prefix///"})
	for _, r := range routes {
		assert.True(t, strings.HasPrefix(r.Path, "/some/random/prefix/api/v1/"))
	}
}
