package ring

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	consulHeartbeats = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_member_consul_heartbeats_total",
		Help: "The total number of heartbeats sent to consul.",
	}, []string{"name"})
	tokensOwned = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_member_ring_tokens_owned",
		Help: "The number of tokens owned in the ring.",
	}, []string{"name"})
	tokensToOwn = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_member_ring_tokens_to_own",
		Help: "The number of tokens to own in the ring.",
	}, []string{"name"})
	shutdownDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_shutdown_duration_seconds",
		Help:    "Duration (in seconds) of cortex shutdown procedure (ie transfer or flush).",
		Buckets: prometheus.ExponentialBuckets(10, 2, 8), // Biggest bucket is 10*2^(9-1) = 2560, or 42 mins.
	}, []string{"op", "status", "name"})
)

// TokenGeneratorFunc is any function that will generate a series
// of tokens to apply to a new lifecycler.
type TokenGeneratorFunc func(numTokens int, taken []uint32) []uint32

// LifecyclerConfig is the config to build a Lifecycler.
type LifecyclerConfig struct {
	RingConfig Config `yaml:"ring,omitempty"`

	// Config for the ingester lifecycle control
	ListenPort               *int          `yaml:"-"`
	NumTokens                int           `yaml:"num_tokens,omitempty"`
	HeartbeatPeriod          time.Duration `yaml:"heartbeat_period,omitempty"`
	ObservePeriod            time.Duration `yaml:"observe_period,omitempty"`
	JoinAfter                time.Duration `yaml:"join_after,omitempty"`
	MinReadyDuration         time.Duration `yaml:"min_ready_duration,omitempty"`
	InfNames                 []string      `yaml:"interface_names"`
	FinalSleep               time.Duration `yaml:"final_sleep"`
	TokensFilePath           string        `yaml:"tokens_file_path,omitempty"`
	JoinIncrementalTransfer  bool          `yaml:"join_incremental_transfer,omitempty"`
	LeaveIncrementalTransfer bool          `yaml:"leave_incremental_transfer,omitempty"`
	MinIncrementalJoinJitter time.Duration `yaml:"min_incremental_join_jitter,omitempty"`
	MaxIncrementalJoinJitter time.Duration `yaml:"max_incremental_join_jitter,omitempty"`
	TransferFinishDelay      time.Duration `yaml:"transfer_finish_delay,omitempty"`

	// For testing, you can override the address and ID of this ingester
	Addr           string `yaml:"address" doc:"hidden"`
	Port           int    `doc:"hidden"`
	ID             string `doc:"hidden"`
	SkipUnregister bool   `yaml:"-"`

	// graveyard for unused flags.
	UnusedFlag  bool `yaml:"claim_on_rollout,omitempty"` // DEPRECATED - left for backwards-compatibility
	UnusedFlag2 bool `yaml:"normalise_tokens,omitempty"` // DEPRECATED - left for backwards-compatibility

	// Function used to generate tokens, can be mocked from
	// tests
	GenerateTokens TokenGeneratorFunc `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *LifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet.
func (cfg *LifecyclerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RingConfig.RegisterFlagsWithPrefix(prefix, f)

	// In order to keep backwards compatibility all of these need to be prefixed
	// with "ingester."
	if prefix == "" {
		prefix = "ingester."
	}

	f.IntVar(&cfg.NumTokens, prefix+"num-tokens", 128, "Number of tokens for each ingester.")
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 5*time.Second, "Period at which to heartbeat to consul.")
	f.DurationVar(&cfg.JoinAfter, prefix+"join-after", 0*time.Second, "Period to wait for a claim from another member; will join automatically after this.")
	f.DurationVar(&cfg.ObservePeriod, prefix+"observe-period", 0*time.Second, "Observe tokens after generating to resolve collisions. Useful when using gossiping ring.")
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 1*time.Minute, "Minimum duration to wait before becoming ready. This is to work around race conditions with ingesters exiting and updating the ring.")
	flagext.DeprecatedFlag(f, prefix+"claim-on-rollout", "DEPRECATED. This feature is no longer optional.")
	flagext.DeprecatedFlag(f, prefix+"normalise-tokens", "DEPRECATED. This feature is no longer optional.")
	f.BoolVar(&cfg.JoinIncrementalTransfer, prefix+"join-incremental-transfer", false, "Request chunks from neighboring ingesters on join. Disables the handoff process when set and ignores the -ingester.join-after flag.")
	f.BoolVar(&cfg.LeaveIncrementalTransfer, prefix+"leave-incremental-transfer", false, "Send chunks to neighboring ingesters on leave. Takes precedence over chunk flushing when set and disables handoff.")
	f.DurationVar(&cfg.MinIncrementalJoinJitter, prefix+"min-incremental-join-jitter", 0*time.Second, "Minimum amount of time to wait before incrementally joining the ring. Allows time to receieve ring updates so two ingesters do not join at once.")
	f.DurationVar(&cfg.MaxIncrementalJoinJitter, prefix+"max-incremental-join-jitter", 2*time.Second, "Maximum amount of time to wait before incrementally joining the ring. Allows time to receieve ring updates so two ingesters do not join at once.")
	f.DurationVar(&cfg.TransferFinishDelay, prefix+"transfer-finish-delay", 5*time.Second, "How long after the incremental join process to notify the target ingesters to clean up any blocked token ranges.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 30*time.Second, "Duration to sleep for before exiting, to ensure metrics are scraped.")
	f.StringVar(&cfg.TokensFilePath, prefix+"tokens-file-path", "", "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")

	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.Strings)(&cfg.InfNames), prefix+"lifecycler.interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.Addr, prefix+"lifecycler.addr", "", "IP address to advertise in consul.")
	f.IntVar(&cfg.Port, prefix+"lifecycler.port", 0, "port to advertise in consul (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.ID, prefix+"lifecycler.ID", hostname, "ID to register into consul.")
}

// IncrementalTransferer controls partial transfer of chunks as the tokens in a
// ring grows or shrinks.
type IncrementalTransferer interface {
	// SendChunkRanges should connect to the target addr and send all chunks for
	// streams whose fingerprint falls within the provided token ranges.
	SendChunkRanges(ctx context.Context, ranges []TokenRange, targetAddr string) error

	// RequestChunkRanges should connect to the target addr and request all chunks
	// for streams whose fingerprint falls within the provided token ranges.
	//
	// If move is true, transferred data should be removed from the target's memory.
	RequestChunkRanges(ctx context.Context, ranges []TokenRange, targetAddr string, move bool) error

	// RequestComplete, when called, indicates that a request of data has been processed.
	// The targetAddr ingester should use the opportunity to do cleanup.
	RequestComplete(ctx context.Context, ranges []TokenRange, targetAddr string)

	// MemoryStreamTokens should return a list of tokens corresponding to in-memory
	// streams for the ingester. Used for reporting purposes.
	MemoryStreamTokens() []uint32
}

// Lifecycler is responsible for managing the lifecycle of entries in the ring.
type Lifecycler struct {
	cfg             LifecyclerConfig
	flushTransferer FlushTransferer
	incTransferer   IncrementalTransferer
	KVStore         kv.Client

	// Controls the lifecycle of the ingester
	quit      chan struct{}
	done      sync.WaitGroup
	actorChan chan func()

	// These values are initialised at startup, and never change
	ID       string
	Addr     string
	RingName string
	RingKey  string

	// Whether to flush if transfer fails on shutdown.
	flushOnShutdown bool

	// We need to remember the ingester state just in case consul goes away and comes
	// back empty.  And it changes during lifecycle of ingester.
	stateMtx            sync.RWMutex
	state               IngesterState
	transitioningTokens Tokens
	tokens              Tokens

	// Controls the ready-reporting
	readyLock sync.Mutex
	startTime time.Time
	ready     bool

	// Keeps stats and ring updated at every heartbeat period
	countersLock          sync.RWMutex
	healthyInstancesCount int
	lastRing              *Desc

	generateTokens TokenGeneratorFunc
}

// NewLifecycler makes and starts a new Lifecycler.
func NewLifecycler(cfg LifecyclerConfig, flushTransferer FlushTransferer, incTransferer IncrementalTransferer, ringName, ringKey string, flushOnShutdown bool) (*Lifecycler, error) {

	addr := cfg.Addr
	if addr == "" {
		var err error
		addr, err = util.GetFirstAddressOf(cfg.InfNames)
		if err != nil {
			return nil, err
		}
	}
	port := cfg.Port
	if port == 0 {
		port = *cfg.ListenPort
	}
	codec := GetCodec()
	store, err := kv.NewClient(cfg.RingConfig.KVStore, codec)
	if err != nil {
		return nil, err
	}

	// We do allow a nil FlushTransferer, but to keep the ring logic easier we assume
	// it's always set, so we use a noop FlushTransferer
	if flushTransferer == nil {
		flushTransferer = NewNoopFlushTransferer()
	}

	l := &Lifecycler{
		cfg:             cfg,
		flushTransferer: flushTransferer,
		incTransferer:   incTransferer,
		KVStore:         store,

		Addr:            fmt.Sprintf("%s:%d", addr, port),
		ID:              cfg.ID,
		RingName:        ringName,
		RingKey:         ringKey,
		flushOnShutdown: flushOnShutdown,

		quit:      make(chan struct{}),
		actorChan: make(chan func()),

		state:          PENDING,
		startTime:      time.Now(),
		generateTokens: cfg.GenerateTokens,
	}

	if l.generateTokens == nil {
		l.generateTokens = GenerateTokens
	}

	tokensToOwn.WithLabelValues(l.RingName).Set(float64(cfg.NumTokens))

	return l, nil
}

// Start the lifecycler
func (i *Lifecycler) Start() {
	i.done.Add(1)
	go i.loop()
}

// CheckReady is used to rate limit the number of ingesters that can be coming or
// going at any one time, by only returning true if all ingesters are active.
// The state latches: once we have gone ready we don't go un-ready
func (i *Lifecycler) CheckReady(ctx context.Context) error {
	i.readyLock.Lock()
	defer i.readyLock.Unlock()

	if i.ready {
		return nil
	}

	// Ingester always take at least minReadyDuration to become ready to work
	// around race conditions with ingesters exiting and updating the ring
	if time.Now().Sub(i.startTime) < i.cfg.MinReadyDuration {
		return fmt.Errorf("waiting for %v after startup", i.cfg.MinReadyDuration)
	}

	desc, err := i.KVStore.Get(ctx, i.RingKey)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error talking to the KV store", "ring", i.RingName, "err", err)
		return fmt.Errorf("error talking to the KV store: %s", err)
	}

	if len(i.getTokens()) == 0 {
		return fmt.Errorf("this instance owns no tokens")
	}

	ringDesc, ok := desc.(*Desc)
	if !ok || ringDesc == nil {
		return fmt.Errorf("no ring returned from the KV store")
	}

	if err := ringDesc.Ready(time.Now(), i.cfg.RingConfig.HeartbeatTimeout); err != nil {
		return err
	}

	i.ready = true
	return nil
}

// GetState returns the state of this ingester.
func (i *Lifecycler) GetState() IngesterState {
	i.stateMtx.RLock()
	defer i.stateMtx.RUnlock()
	return i.state
}

func (i *Lifecycler) setState(state IngesterState) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	i.state = state
}

// ChangeState of the ingester, for use off of the loop() goroutine.
func (i *Lifecycler) ChangeState(ctx context.Context, state IngesterState) error {
	err := make(chan error)
	i.actorChan <- func() {
		err <- i.changeState(ctx, state)
	}
	return <-err
}

func (i *Lifecycler) getTransitioningTokens() Tokens {
	i.stateMtx.RLock()
	defer i.stateMtx.RUnlock()
	ret := make([]uint32, len(i.transitioningTokens))
	copy(ret, i.transitioningTokens)
	return ret
}

func (i *Lifecycler) getTokens() Tokens {
	i.stateMtx.RLock()
	defer i.stateMtx.RUnlock()
	ret := make([]uint32, len(i.tokens))
	copy(ret, i.tokens)
	return ret
}

// addToken adds a new token into the ring. The newest token must be larger
// than the last token in the set to maintain internal sorted order.
func (i *Lifecycler) addToken(token uint32) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	if len(i.tokens) > 0 && token <= i.tokens[len(i.tokens)-1] {
		panic("addToken invoked without maintaining token order")
	}

	tokensOwned.WithLabelValues(i.RingName).Inc()
	i.tokens = append(i.tokens, token)
}

func (i *Lifecycler) removeToken(token uint32) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	for idx, tok := range i.tokens {
		if tok == token {
			tokensOwned.WithLabelValues(i.RingName).Dec()
			i.tokens = append(i.tokens[:idx], i.tokens[idx+1:]...)
			return
		}
	}
}

func (i *Lifecycler) setTransitioningTokens(tokens Tokens) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	i.transitioningTokens = make([]uint32, len(tokens))
	copy(i.transitioningTokens, tokens)
}

func (i *Lifecycler) setTokens(tokens Tokens) {
	tokensOwned.WithLabelValues(i.RingName).Set(float64(len(tokens)))

	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	i.tokens = make([]uint32, len(tokens))
	copy(i.tokens, tokens)

	if i.cfg.TokensFilePath != "" {
		if err := i.tokens.StoreToFile(i.cfg.TokensFilePath); err != nil {
			level.Error(util.Logger).Log("msg", "error storing tokens to disk", "path", i.cfg.TokensFilePath, "err", err)
		}
	}
}

// ClaimTokensFor takes all the tokens for the supplied ingester and assigns them to this ingester.
//
// For this method to work correctly (especially when using gossiping), source ingester (specified by
// ingesterID) must be in the LEAVING state, otherwise ring's merge function may detect token conflict and
// assign token to the wrong ingester. While we could check for that state here, when this method is called,
// transfers have already finished -- it's better to check for this *before* transfers start.
func (i *Lifecycler) ClaimTokensFor(ctx context.Context, ingesterID string) error {
	err := make(chan error)

	i.actorChan <- func() {
		var tokens Tokens

		claimTokens := func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc, ok := in.(*Desc)
			if !ok || ringDesc == nil {
				return nil, false, fmt.Errorf("Cannot claim tokens in an empty ring")
			}

			tokens = ringDesc.ClaimTokens(ingesterID, i.ID)
			// update timestamp to give gossiping client a chance register ring change.
			ing := ringDesc.Ingesters[i.ID]
			ing.Timestamp = time.Now().Unix()
			ringDesc.Ingesters[i.ID] = ing

			return ringDesc, true, nil
		}

		if err := i.KVStore.CAS(ctx, i.RingKey, claimTokens); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to write to the KV store", "ring", i.RingName, "err", err)
		}

		i.setTokens(tokens)
		err <- nil
	}

	return <-err
}

// HealthyInstancesCount returns the number of healthy instances in the ring, updated
// during the last heartbeat period
func (i *Lifecycler) HealthyInstancesCount() int {
	i.countersLock.RLock()
	defer i.countersLock.RUnlock()

	return i.healthyInstancesCount
}

// Shutdown the lifecycle.  It will:
// - send chunks to another ingester, if it can.
// - otherwise, flush chunks to the chunk store.
// - remove config from Consul.
// - block until we've successfully shutdown.
func (i *Lifecycler) Shutdown() {
	i.flushTransferer.StopIncomingRequests()

	// closing i.quit triggers loop() to exit, which in turn will trigger
	// the removal of our tokens etc
	close(i.quit)
	i.done.Wait()
}

func (i *Lifecycler) loop() {
	defer func() {
		level.Info(util.Logger).Log("msg", "member.loop() exited gracefully", "ring", i.RingName)
		i.done.Done()
	}()

	// First, see if we exist in the cluster, update our state to match if we do,
	// and add ourselves (without tokens) if we don't.
	if err := i.initRing(context.Background()); err != nil {
		level.Error(util.Logger).Log("msg", "failed to join the ring", "ring", i.RingName, "err", err)
		os.Exit(1)
	}

	// We do various period tasks
	autoJoinTimer := time.NewTimer(i.cfg.JoinAfter)
	autoJoinAfter := autoJoinTimer.C
	var observeChan <-chan time.Time = nil

	heartbeatTicker := time.NewTicker(i.cfg.HeartbeatPeriod)
	defer heartbeatTicker.Stop()

	if i.cfg.JoinIncrementalTransfer {
		if !autoJoinTimer.Stop() {
			// Drain the value if one was available.
			<-autoJoinTimer.C
		}

		level.Info(util.Logger).Log("msg", "joining cluster")
		if err := i.waitCleanRing(context.Background()); err != nil {
			// If this fails, we'll get spill over of data, but we can safely continue here.
			level.Error(util.Logger).Log("msg", "failed to wait for a clean ring to join", "err", err)
		}

		if err := i.autoJoin(context.Background(), JOINING); err != nil {
			level.Error(util.Logger).Log("msg", "failed to pick tokens in KV store", "err", err)
			os.Exit(1)
		}

		if err := i.joinIncrementalTransfer(context.Background()); err != nil {
			level.Error(util.Logger).Log("msg", "failed to obtain chunks on join", "err", err)
		}
	}

loop:
	for {
		select {
		case <-autoJoinAfter:
			level.Debug(util.Logger).Log("msg", "JoinAfter expired", "ring", i.RingName)
			// Will only fire once, after auto join timeout.  If we haven't entered "JOINING" state,
			// then pick some tokens and enter ACTIVE state.
			if i.GetState() == PENDING {
				level.Info(util.Logger).Log("msg", "auto-joining cluster after timeout", "ring", i.RingName)

				if i.cfg.ObservePeriod > 0 {
					// let's observe the ring. By using JOINING state, this ingester will be ignored by LEAVING
					// ingesters, but we also signal that it is not fully functional yet.
					if err := i.autoJoin(context.Background(), JOINING); err != nil {
						level.Error(util.Logger).Log("msg", "failed to pick tokens in the KV store", "ring", i.RingName, "err", err)
						os.Exit(1)
					}

					level.Info(util.Logger).Log("msg", "observing tokens before going ACTIVE", "ring", i.RingName)
					observeChan = time.After(i.cfg.ObservePeriod)
				} else {
					if err := i.autoJoin(context.Background(), ACTIVE); err != nil {
						level.Error(util.Logger).Log("msg", "failed to pick tokens in the KV store", "ring", i.RingName, "err", err)
						os.Exit(1)
					}
				}
			}

		case <-observeChan:
			// if observeChan is nil, this case is ignored. We keep updating observeChan while observing the ring.
			// When observing is done, observeChan is set to nil.

			observeChan = nil
			if s := i.GetState(); s != JOINING {
				level.Error(util.Logger).Log("msg", "unexpected state while observing tokens", "state", s, "ring", i.RingName)
			}

			if i.verifyTokens(context.Background()) {
				level.Info(util.Logger).Log("msg", "token verification successful", "ring", i.RingName)

				err := i.changeState(context.Background(), ACTIVE)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to set state to ACTIVE", "ring", i.RingName, "err", err)
				}
			} else {
				level.Info(util.Logger).Log("msg", "token verification failed, observing", "ring", i.RingName)
				// keep observing
				observeChan = time.After(i.cfg.ObservePeriod)
			}
		case <-heartbeatTicker.C:
			consulHeartbeats.WithLabelValues(i.RingName).Inc()
			if err := i.updateConsul(context.Background()); err != nil {
				level.Error(util.Logger).Log("msg", "failed to write to the KV store, sleeping", "ring", i.RingName, "err", err)
			}

		case f := <-i.actorChan:
			f()

		case <-i.quit:
			break loop
		}
	}

	// Mark ourselved as Leaving so no more samples are send to us.
	i.changeState(context.Background(), LEAVING)

	// Do the transferring / flushing on a background goroutine so we can continue
	// to heartbeat to consul.
	done := make(chan struct{})
	go func() {
		i.processShutdown(context.Background())
		close(done)
	}()

heartbeatLoop:
	for {
		select {
		case <-heartbeatTicker.C:
			consulHeartbeats.WithLabelValues(i.RingName).Inc()
			if err := i.updateConsul(context.Background()); err != nil {
				level.Error(util.Logger).Log("msg", "failed to write to the KV store, sleeping", "ring", i.RingName, "err", err)
			}

		case <-done:
			break heartbeatLoop
		}
	}

	if !i.cfg.SkipUnregister {
		if err := i.unregister(context.Background()); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to unregister from the KV store", "ring", i.RingName, "err", err)
			os.Exit(1)
		}
		level.Info(util.Logger).Log("msg", "instance removed from the KV store", "ring", i.RingName)
	}
}

// waitCleanRing incrementally reads from the KV store and waits
// until there are no JOINING or LEAVING ingesters.
func (i *Lifecycler) waitCleanRing(ctx context.Context) error {
	max := i.cfg.MaxIncrementalJoinJitter.Milliseconds()
	min := i.cfg.MinIncrementalJoinJitter.Milliseconds()

	// Sleep for a random period between [min, max). Used to stagger multiple nodes
	// all waiting for the ring to be clean.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sleepMs := r.Int63n(max-min) + min
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)

	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
	})

	for backoff.Ongoing() {
		select {
		case <-i.quit:
			return errors.New("shutting down")
		default:
		}

		ok, err := i.checkCleanRing(ctx)
		if err != nil {
			return err
		} else if ok {
			return nil
		}

		backoff.Wait()
	}

	return backoff.Err()
}

// checkCleanRing returns true when the ring has no JOINING
// or LEAVING ingesters. "clean" implies that it is safe for a
// new node to join.
func (i *Lifecycler) checkCleanRing(ctx context.Context) (bool, error) {
	d, err := i.KVStore.Get(ctx, i.RingKey)
	if err != nil {
		return false, err
	} else if d == nil {
		return false, nil
	}

	desc, ok := d.(*Desc)
	if !ok {
		return false, fmt.Errorf("could not convert ring to Desc")
	}

	unclean := 0
	for k, ing := range desc.Ingesters {
		if k == i.ID {
			continue
		}
		if ing.State == JOINING || ing.State == LEAVING {
			unclean++
		}
	}

	return unclean == 0, nil
}

// initRing is the first thing we do when we start. It:
// - add an ingester entry to the ring
// - copies out our state and tokens if they exist
func (i *Lifecycler) initRing(ctx context.Context) error {
	var (
		ringDesc       *Desc
		tokensFromFile Tokens
		err            error
	)

	if i.cfg.TokensFilePath != "" {
		tokensFromFile, err = LoadTokensFromFile(i.cfg.TokensFilePath)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error in getting tokens from file", "err", err)
		}
	} else {
		level.Info(util.Logger).Log("msg", "not loading tokens from file, tokens file path is empty")
	}

	err = i.KVStore.CAS(ctx, i.RingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.ID]
		if !ok {
			state := i.GetState()
			incremental := i.incrementalFromState(state)

			// We use the tokens from the file only if it does not exist in the ring yet.
			if len(tokensFromFile) > 0 {
				level.Info(util.Logger).Log("msg", "adding tokens from file", "num_tokens", len(tokensFromFile))
				if len(tokensFromFile) >= i.cfg.NumTokens {
					i.setState(ACTIVE)
				}
				ringDesc.AddIngester(i.ID, i.Addr, tokensFromFile, i.GetState(), incremental)
				i.setTokens(tokensFromFile)
				return ringDesc, true, nil
			}

			// Either we are a new ingester, or consul must have restarted
			level.Info(util.Logger).Log("msg", "instance not found in ring, adding with no tokens", "ring", i.RingName)
			ringDesc.AddIngester(i.ID, i.Addr, nil, state, incremental)
			return ringDesc, true, nil
		}

		// If the ingester failed to clean it's ring entry up in can leave it's state in LEAVING.
		// Move it into ACTIVE to ensure the ingester joins the ring.
		if ingesterDesc.State == LEAVING && len(ingesterDesc.Tokens) == i.cfg.NumTokens {
			ingesterDesc.State = ACTIVE
		}

		// We exist in the ring, so assume the ring is right and copy out tokens & state out of there.
		i.setState(ingesterDesc.State)
		tokens, _ := ringDesc.TokensFor(i.ID)
		i.setTokens(tokens)

		level.Info(util.Logger).Log("msg", "existing entry found in ring", "state", i.GetState(), "tokens", len(tokens), "ring", i.RingName)
		// we haven't modified the ring, don't try to store it.
		return nil, true, nil
	})

	// Update counters and ring.
	if err == nil {
		i.updateCounters(ringDesc)
		i.updateLastRing(ringDesc)
	}

	return err
}

// Verifies that tokens that this ingester has registered to the ring still belong to it.
// Gossiping ring may change the ownership of tokens in case of conflicts.
// If ingester doesn't own its tokens anymore, this method generates new tokens and puts them to the ring.
func (i *Lifecycler) verifyTokens(ctx context.Context) bool {
	result := false

	err := i.KVStore.CAS(ctx, i.RingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		// At this point, we should have the same tokens as we have registered before
		ringTokens, takenTokens := ringDesc.TokensFor(i.ID)

		if !i.compareTokens(ringTokens) {
			// uh, oh... our tokens are not our anymore. Let's try new ones.
			needTokens := i.cfg.NumTokens - len(ringTokens)

			level.Info(util.Logger).Log("msg", "generating new tokens", "count", needTokens, "ring", i.RingName)
			newTokens := i.generateTokens(needTokens, takenTokens)

			addTokens := Tokens(append(ringTokens, newTokens...))
			sort.Sort(addTokens)

			state := i.GetState()
			incremental := i.incrementalFromState(state)
			ringDesc.AddIngester(i.ID, i.Addr, addTokens, state, incremental)

			i.setTokens(addTokens)

			return ringDesc, true, nil
		}

		// all is good, this ingester owns its tokens
		result = true
		return nil, true, nil
	})

	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to verify tokens", "ring", i.RingName, "err", err)
		return false
	}

	return result
}

func (i *Lifecycler) compareTokens(fromRing Tokens) bool {
	sort.Sort(fromRing)

	tokens := i.getTokens()
	sort.Sort(tokens)

	if len(tokens) != len(fromRing) {
		return false
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i] != fromRing[i] {
			return false
		}
	}
	return true
}

// autoJoin selects random tokens & moves state to targetState
func (i *Lifecycler) autoJoin(ctx context.Context, targetState IngesterState) error {
	var ringDesc *Desc

	err := i.KVStore.CAS(ctx, i.RingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		// At this point, we should not have any tokens, and we should be in PENDING state.
		myTokens, takenTokens := ringDesc.TokensFor(i.ID)
		if len(myTokens) > 0 {
			level.Error(util.Logger).Log("msg", "tokens already exist for this instance - wasn't expecting any!", "num_tokens", len(myTokens), "ring", i.RingName)
		}

		newTokens := i.generateTokens(i.cfg.NumTokens-len(myTokens), takenTokens)
		i.setState(targetState)

		// When we're incrementally joining the ring, tokens are only inserted
		// incrementally during the join process.
		insertTokens := newTokens
		if i.cfg.JoinIncrementalTransfer {
			insertTokens = nil
		}

		state := i.GetState()
		incremental := i.incrementalFromState(state)
		ringDesc.AddIngester(i.ID, i.Addr, insertTokens, state, incremental)

		tokens := append(myTokens, newTokens...)
		sort.Sort(Tokens(tokens))

		if i.cfg.JoinIncrementalTransfer {
			i.setTransitioningTokens(tokens)
		} else {
			i.setTokens(tokens)
		}

		return ringDesc, true, nil
	})

	// Update counters and ring.
	if err == nil {
		i.updateCounters(ringDesc)
		i.updateLastRing(ringDesc)
	}

	return err
}

// updateConsul updates our entries in consul, heartbeating and dealing with
// consul restarts.
func (i *Lifecycler) updateConsul(ctx context.Context) error {
	var ringDesc *Desc

	err := i.KVStore.CAS(ctx, i.RingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.ID]
		if !ok {
			// consul must have restarted
			level.Info(util.Logger).Log("msg", "found empty ring, inserting tokens", "ring", i.RingName)
			state := i.GetState()
			incremental := i.incrementalFromState(state)
			ringDesc.AddIngester(i.ID, i.Addr, i.getTokens(), state, incremental)
		} else {
			ingesterDesc.Timestamp = time.Now().Unix()
			ingesterDesc.State = i.GetState()
			ingesterDesc.Addr = i.Addr
			ingesterDesc.Incremental = i.incrementalFromState(ingesterDesc.State)
			ringDesc.Ingesters[i.ID] = ingesterDesc
		}

		// Re-sync token states for the current lifecycler if they've changed.
		ringDesc.SetIngesterTokens(i.ID, i.getTokens())
		return ringDesc, true, nil
	})

	// Update counters and ring.
	if err == nil {
		i.updateCounters(ringDesc)
		i.updateLastRing(ringDesc)
	}

	return err
}

// incrementalFromState determines if the ingester should be tagged as being in an
// "incremental state", flagging to the distributors to treat JOINING/LEAVING as
// healthy.
func (i *Lifecycler) incrementalFromState(state IngesterState) bool {
	return (state == JOINING && i.cfg.JoinIncrementalTransfer) ||
		(state == LEAVING && i.cfg.LeaveIncrementalTransfer)
}

// changeState updates consul with state transitions for us.  NB this must be
// called from loop()!  Use ChangeState for calls from outside of loop().
func (i *Lifecycler) changeState(ctx context.Context, state IngesterState) error {
	currState := i.GetState()
	// Only the following state transitions can be triggered externally
	if !((currState == PENDING && state == JOINING) || // triggered by TransferChunks at the beginning
		(currState == JOINING && state == PENDING) || // triggered by TransferChunks on failure
		(currState == JOINING && state == ACTIVE) || // triggered by TransferChunks on success
		(currState == PENDING && state == ACTIVE) || // triggered by autoJoin
		(currState == ACTIVE && state == LEAVING)) { // triggered by shutdown
		return fmt.Errorf("Changing instance state from %v -> %v is disallowed", currState, state)
	}

	level.Info(util.Logger).Log("msg", "changing instance state from", "old_state", currState, "new_state", state, "ring", i.RingName)
	i.setState(state)
	return i.updateConsul(ctx)
}

// getLastRing returns a copy of the last ring saved.
func (i *Lifecycler) getLastRing() *Desc {
	i.countersLock.Lock()
	defer i.countersLock.Unlock()

	return i.lastRing
}

func (i *Lifecycler) updateCounters(ringDesc *Desc) {
	// Count the number of healthy instances for Write operation
	healthyInstancesCount := 0

	if ringDesc != nil {
		for _, ingester := range ringDesc.Ingesters {
			if ingester.IsHealthy(Write, i.cfg.RingConfig.HeartbeatTimeout) {
				healthyInstancesCount++
			}
		}
	}

	// Update counters
	i.countersLock.Lock()
	i.healthyInstancesCount = healthyInstancesCount
	i.countersLock.Unlock()
}

// FlushOnShutdown returns if flushing is enabled if transfer fails on a shutdown.
func (i *Lifecycler) FlushOnShutdown() bool {
	return i.flushOnShutdown
}

// SetFlushOnShutdown enables/disables flush on shutdown if transfer fails.
// Passing 'true' enables it, and 'false' disabled it.
func (i *Lifecycler) SetFlushOnShutdown(flushOnShutdown bool) {
	i.flushOnShutdown = flushOnShutdown
}

func (i *Lifecycler) updateLastRing(ringDesc *Desc) {
	i.countersLock.Lock()
	defer i.countersLock.Unlock()

	i.lastRing = ringDesc
}

func (i *Lifecycler) processShutdown(ctx context.Context) {
	flushRequired := i.flushOnShutdown
	transferStart := time.Now()

	if i.cfg.LeaveIncrementalTransfer {
		if err := i.leaveIncrementalTransfer(ctx); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to incrementally transfer chunks to another ingester", "err", err)
			shutdownDuration.WithLabelValues("incremental_transfer", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())
		} else {
			// The ingester may still have data that wasn't transferred if it got any
			// unexpected writes (or if there's a bug!). We'll keep flushRequired as true
			// to make sure this remaining data gets flushed.
			shutdownDuration.WithLabelValues("incremental_transfer", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
		}
	} else {
		if err := i.flushTransferer.TransferOut(ctx); err != nil {
			if err == ErrTransferDisabled {
				level.Info(util.Logger).Log("msg", "transfers are disabled")
			} else {
				level.Error(util.Logger).Log("msg", "failed to transfer chunks to another instance", "ring", i.RingName, "err", err)
				shutdownDuration.WithLabelValues("transfer", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())
			}
		} else {
			flushRequired = false
			shutdownDuration.WithLabelValues("transfer", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
		}
	}

	if flushRequired {
		flushStart := time.Now()
		i.flushTransferer.Flush()
		shutdownDuration.WithLabelValues("flush", "success", i.RingName).Observe(time.Since(flushStart).Seconds())
	}

	// Sleep so the shutdownDuration metric can be collected.
	time.Sleep(i.cfg.FinalSleep)
}

// unregister removes our entry from consul.
func (i *Lifecycler) unregister(ctx context.Context) error {
	level.Debug(util.Logger).Log("msg", "unregistering instance from ring", "ring", i.RingName)

	return i.KVStore.CAS(ctx, i.RingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.RemoveIngester(i.ID)
		return ringDesc, true, nil
	})
}
