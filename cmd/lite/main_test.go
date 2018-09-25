package main

import (
	"testing"
)

func TestFlags(t *testing.T) {
	getConfigsFromCommandLine()
	t.Log("Success (no panics for 'flag redefined')")
}
