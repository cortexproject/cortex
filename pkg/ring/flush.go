package ring

// FlushTransferer controls the shutdown of an instance in the ring.
// Methods on this interface are called when lifecycler is stopping.
// At that point, it no longer runs the "actor loop", but it keeps updating heartbeat in the ring.
// Ring entry is in LEAVING state.
type FlushTransferer interface {
	Flush()
}

// NoopFlushTransferer is a FlushTransferer which does nothing and can
// be used in cases we don't need one
type NoopFlushTransferer struct{}

// NewNoopFlushTransferer makes a new NoopFlushTransferer
func NewNoopFlushTransferer() *NoopFlushTransferer {
	return &NoopFlushTransferer{}
}

// Flush is a noop
func (t *NoopFlushTransferer) Flush() {}
