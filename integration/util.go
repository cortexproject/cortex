package main

import (
	"github.com/cortexproject/cortex/integration/framework"
)

var (
	// Expose some utilities form the framework so that we don't have to prefix them
	// with the package name in tests.
	mergeFlags      = framework.MergeFlags
	newDynamoClient = framework.NewDynamoClient
	generateSeries  = framework.GenerateSeries
)
