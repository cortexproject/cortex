package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlags(t *testing.T) {
	assert.NotPanics(t, getConfigsFromCommandLine)
}
