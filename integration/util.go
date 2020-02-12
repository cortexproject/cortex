package main

import (
	e2e "github.com/cortexproject/cortex/integration/framework"
	e2edb "github.com/cortexproject/cortex/integration/framework/db"
)

var (
	// Expose some utilities form the framework so that we don't have to prefix them
	// with the package name in tests.
	mergeFlags      = e2e.MergeFlags
	newDynamoClient = e2edb.NewDynamoClient
	generateSeries  = e2e.GenerateSeries
)
