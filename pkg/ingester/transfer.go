package ingester

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/cortexproject/cortex/pkg/ring"
)

// TransferOut finds an ingester in PENDING state and transfers our chunks to it.
// Called as part of the ingester shutdown process.
func (i *Ingester) TransferOut(ctx context.Context) error {
	// The blocks storage doesn't support blocks transferring.
	level.Info(i.logger).Log("msg", "transfer between a LEAVING ingester and a PENDING one is not supported for the blocks storage")
	return ring.ErrTransferDisabled
}
