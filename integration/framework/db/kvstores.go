package e2edb

import (
	e2e "github.com/cortexproject/cortex/integration/framework"
)

func NewConsul() *e2e.Service {
	return e2e.NewService(
		"consul",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"consul:0.9",
		e2e.NetworkName,
		[]int{},
		nil,
		// Run consul in "dev" mode so that the initial leader election is immediate
		e2e.NewCommand("agent", "-server", "-client=0.0.0.0", "-dev", "-log-level=err"),
		nil,
	)
}
