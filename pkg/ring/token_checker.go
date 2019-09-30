package ring

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// TokenCheckerConfig is the config to configure a TokenChecker.
type TokenCheckerConfig struct {
	CheckOnInterval time.Duration `yaml:"check_on_interval"`
}

// RegisterFlags adds flags required to configure a TokenChecker to
// the provided FlagSet.
func (c *TokenCheckerConfig) RegisterFlags(f *flag.FlagSet) {
	c.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config a TokenChecker to
// the given FlagSet, prefixing each flag with the value provided by prefix.
func (c *TokenCheckerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&c.CheckOnInterval, prefix+"token-checker.check-on-interval", time.Duration(0), "Period with which to check that all in-memory streams fall within expected token ranges. 0 to disable.")
}

// A TokenChecker is responsible for validating that streams written to an
// ingester have tokens that fall within an expected range of values.
// Appropriate values depend on where an ingester's tokens are placed in the
// ring and what its neighbors are.
//
// Checking that newly created streams fall within the expected token
// ranges ensures that writes to the ingester are distributed properly,
// while checking for validity of streams on an interval ensures that
// an ingester's memory only contains appropriate tokens as the ingesters
// in the ring change over time.
type TokenChecker struct {
	cfg        TokenCheckerConfig
	ringCfg    Config
	lifecycler *Lifecycler

	// Lifecycle control
	ctx    context.Context
	cancel context.CancelFunc

	// Updated throughout the lifetime of a TokenChecker
	mut            sync.Mutex
	expectedRanges []TokenRange

	unexpectedStreamsHandler func(streamTokens []uint32)
}

// NewTokenChecker makes and starts a new TokenChecker. unexpectedStreamsHandler will be
// invoked by TokenChecker whenever CheckAllTokens is called, even when no unexpected
// tokens are found. If nil, unexpectedStreamsHandler is a no-op.
func NewTokenChecker(cfg TokenCheckerConfig, ringConfig Config, lc *Lifecycler, unexpectedStreamsHandler func(streamTokens []uint32)) *TokenChecker {
	tc := &TokenChecker{
		cfg:                      cfg,
		ringCfg:                  ringConfig,
		lifecycler:               lc,
		unexpectedStreamsHandler: unexpectedStreamsHandler,
	}
	tc.ctx, tc.cancel = context.WithCancel(context.Background())

	go tc.loop()
	return tc
}

// Shutdown stops the Token Checker. It will stop watching the ring
// for changes and stop checking that tokens are valid on an interval.
func (tc *TokenChecker) Shutdown() {
	tc.cancel()
}

// TokenExpected iterates over all expected ranges and returns true
// when the token falls within one of those ranges.
func (tc *TokenChecker) TokenExpected(token uint32) bool {
	tc.mut.Lock()
	defer tc.mut.Unlock()

	for _, rg := range tc.expectedRanges {
		if rg.Contains(token) {
			return true
		}
	}

	return false
}

// CheckAllStreams invokes TokenExpected for all current untransferred
// tokens found in the stored IncrementalTransferer. Invokes
// tc.InvalidTokenHandler when an invalid token was found. Returns true when
// all tokens are valid.
func (tc *TokenChecker) CheckAllStreams() bool {
	var invalid []uint32

	toks := tc.lifecycler.incTransferer.MemoryStreamTokens()
	for _, tok := range toks {
		valid := tc.TokenExpected(tok)
		if !valid {
			invalid = append(invalid, tok)
		}
	}

	numInvalid := len(invalid)

	if tc.unexpectedStreamsHandler != nil {
		tc.unexpectedStreamsHandler(invalid)
	}

	return numInvalid == 0
}

// syncRing syncs the latest ring from the lifecycler with expected ranges.
func (tc *TokenChecker) syncRing() {
	r := tc.lifecycler.getLastRing()
	if r == nil {
		// Wait for there to be a ring available to use
		return
	}
	tc.updateExpectedRanges(r)
}

// updateExpectedRanges goes through the ring and finds all expected ranges
// given the current set of tokens in a Lifecycler.
func (tc *TokenChecker) updateExpectedRanges(ring *Desc) {
	var expected []TokenRange

	healthy := ring.HealthChecker(Read, tc.ringCfg.HeartbeatTimeout)
	n := ring.GetNavigator()

	tokens := tc.lifecycler.getTokens()
	for _, tok := range tokens {
		for replica := 0; replica < tc.ringCfg.ReplicationFactor; replica++ {
			endRanges, err := n.Predecessors(tok, replica, healthy)
			if err != nil {
				level.Error(util.Logger).Log("msg", "unable to update expected token ranges", "err", err)
				return
			}

			for _, endRange := range endRanges {
				startRange, err := n.Neighbor(endRange.Token, -1, false, healthy)
				if err != nil {
					level.Error(util.Logger).Log("msg", "unable to update expected token ranges", "err", err)
					return
				}

				expected = append(expected, TokenRange{
					From: startRange.Token,
					To:   endRange.Token,
				})
			}
		}
	}

	tc.mut.Lock()
	defer tc.mut.Unlock()
	tc.expectedRanges = expected
}

// wrapTicker returns a channel that ticks on the duration d. If d
// is zero, returns a channel that never produces a value.
func wrapTicker(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}

	ticker := time.NewTicker(d)
	return ticker.C, ticker.Stop
}

// loop will invoke CheckAllTokens based on the check interval.
func (tc *TokenChecker) loop() {
	check, closeCheck := wrapTicker(tc.cfg.CheckOnInterval)
	defer closeCheck()

loop:
	for {
		select {
		case <-check:
			tc.syncRing()
			tc.CheckAllStreams()
		case <-tc.ctx.Done():
			break loop
		}
	}
}
