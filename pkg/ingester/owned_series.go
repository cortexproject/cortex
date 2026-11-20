package ingester

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"slices"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
)

// seriesOwnership is the ownership state handed to a tenant's active series
// tracker: a ring's sorted tokens and a parallel bitmap of the token positions
// this ingester owns. See ring.Ring.OwnedTokenPositions.
type seriesOwnership struct {
	tokens    []uint32
	positions []bool
	// fingerprint identifies the ownership to the tracker, which re-evaluates
	// every series it holds when it changes. It changes only when the bitmap
	// does. Zero means ownership is unknown, and the tracker then keeps what it
	// had, which before any ownership has been applied counts every series as
	// owned.
	fingerprint uint64
	// source identifies the ring, or subring, the bitmap was computed from. It
	// changes whenever the bitmap may have, which is more often than the bitmap
	// actually does: during a rolling restart every state change anywhere in
	// the ring changes the whole ring's source, though it changes this
	// ingester's ownership only in a few token ranges, if at all.
	source uint64
	// shardSize is the shuffle-shard size the ownership was computed for, or
	// zero for ownership over the whole ring.
	shardSize int
	// viewRead is the lifecycler ring read the ownership is up to date as of. A
	// series is only judged by an ownership whose view was read after it arrived.
	viewRead uint32
}

// sameBitmap reports whether tokens and positions are the bitmap o already holds.
//
// Tokens are compared by value. A topology change rebuilds every subring with a
// new token list, even for a shard whose members did not change, and comparing
// the lists' contents is far cheaper than re-evaluating every series of a tenant
// whose ownership is the same.
func (o seriesOwnership) sameBitmap(tokens []uint32, positions []bool) bool {
	return slices.Equal(o.tokens, tokens) && slices.Equal(o.positions, positions)
}

// withBitmap returns the ownership for a bitmap computed from source. When the
// bitmap is the one o already holds, o is kept, so that the trackers it has
// been applied to are not made to re-evaluate every series for nothing.
func (o seriesOwnership) withBitmap(tokens []uint32, positions []bool, source uint64) seriesOwnership {
	if o.fingerprint != 0 && o.sameBitmap(tokens, positions) {
		// Keep the fingerprint, but adopt the new slices, so that the previous
		// ring's can be freed.
		o.tokens, o.positions, o.source = tokens, positions, source
		return o
	}
	return seriesOwnership{tokens: tokens, positions: positions, fingerprint: source, source: source}
}

// ownedSeriesState computes series ownership for each tenant from the ring the
// distributor routes that tenant's writes within: the whole ring, or for a tenant
// with a shuffle-shard size smaller than the ring, the tenant's subring.
//
// It is only accessed by the goroutine running ownedSeriesLoop.
type ownedSeriesState struct {
	// ring is built from the ring the lifecycler reads on every heartbeat, rather
	// than by watching the KV store a second time. It gives the same answers as
	// the distributor's ring, including its shuffle-shard subrings. It is also
	// the same view of the ring the limiter computes local limits from, so a
	// ring change reaches the local limit and the owned count together.
	ring *ring.Ring

	// ringDesc is the lifecycler's ring that ring was last updated from, and
	// viewRead the number of the lifecycler read it is up to date as of.
	ringDesc *ring.Desc
	viewRead uint32

	// wholeRing is the ownership over the whole ring, shared by every tenant
	// routed over it.
	wholeRing seriesOwnership

	// tenants holds the ownership last applied to each tenant's tracker, for
	// every tenant seen by the last recount. For a tenant routed over the whole
	// ring it shares wholeRing's slices, so it costs nothing extra.
	tenants map[string]seriesOwnership
}

func newOwnedSeriesState(r *ring.Ring) *ownedSeriesState {
	return &ownedSeriesState{
		ring:    r,
		tenants: map[string]seriesOwnership{},
	}
}

// refreshOwnedSeriesRing brings the owned-series ring up to date with the ring
// the lifecycler last read. The lifecycler replaces its copy only when something
// ownership depends on changed, so an unchanged pointer means nothing to do.
func (i *Ingester) refreshOwnedSeriesRing() {
	s := i.ownedSeries
	desc, read := i.lifecycler.RingDesc()
	if desc == nil {
		return
	}
	if desc != s.ringDesc {
		s.ring.UpdateDesc(desc)
		s.ringDesc = desc
	}
	s.viewRead = read
}

// ownedSeriesLoop runs all owned-series work, on its own goroutine so that it
// never holds up the ingester's update loop, whose rate limiter ticks assume a
// steady interval. All of ownedSeriesState, and each tracker's ownership state,
// is only accessed from here.
//
// Every ownedSeriesCheckPeriod it recounts the tenants whose ownership changed,
// because the lifecycler read a changed ring or a tenant's shard size changed,
// which is cheap when nothing did. Every active series update period it recounts
// every tracker in full.
func (i *Ingester) ownedSeriesLoop(ctx context.Context) error {
	var checkTicker, updateTicker <-chan time.Time
	if period := i.cfg.ownedSeriesCheckPeriod; period > 0 {
		t := time.NewTicker(period)
		defer t.Stop()
		checkTicker = t.C
	}
	if period := i.cfg.ActiveSeriesMetricsUpdatePeriod; period > 0 {
		t := time.NewTicker(period)
		defer t.Stop()
		updateTicker = t.C
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-checkTicker:
			i.recountOwnedSeriesIfChanged()
		case <-updateTicker:
			i.updateOwnedSeries()
		}
	}
}

// updateOwnedSeries recounts every tenant's tracker against its current
// ownership, judging the series too new for the previous view of the ring, and
// stores the instance-level owned count.
func (i *Ingester) updateOwnedSeries() {
	s := i.ownedSeries
	purgeTime := time.Now().Add(-i.cfg.ActiveSeriesMetricsIdleTimeout)

	// Resolved once and applied to each tenant. The walk over the ring, or a
	// tenant's subring, only runs when that ring has changed.
	wholeRing := i.ringOwnership()
	seen := map[string]struct{}{}

	for _, userID := range i.getTSDBUsers() {
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}

		ownership := i.tenantOwnership(userID, wholeRing)
		userDB.activeSeries.UpdateMetrics(purgeTime, ownership.tokens, ownership.positions, ownership.fingerprint, ownership.viewRead)
		i.metrics.ownedSeriesPerUser.WithLabelValues(userID).Set(float64(userDB.activeSeries.Owned()))
		if ownership.fingerprint != 0 {
			s.tenants[userID] = ownership
		}
		seen[userID] = struct{}{}
	}

	i.forgetOwnedSeriesTenants(seen)
	// Recalculated from scratch each cycle, which avoids drift from edge cases
	// such as missed decrements during ring changes or tenant deletions.
	i.storeInstanceOwnedSnapshot()
}

// ringOwnership returns this ingester's ownership over the whole ring, computing
// it only when the ring has changed since it was last computed.
//
// Ownership is unknown, and so not applied, while this ingester is not in the
// ring, which is the case at startup until it has joined. Without this, every
// series created in that window would count as owned by nobody.
func (i *Ingester) ringOwnership() seriesOwnership {
	s := i.ownedSeries
	i.refreshOwnedSeriesRing()

	source := s.ring.OwnershipFingerprint()
	if source == 0 {
		return seriesOwnership{}
	}
	if source == s.wholeRing.source {
		// Unchanged, so up to date as of the current view.
		s.wholeRing.viewRead = s.viewRead
		return s.wholeRing
	}
	if _, err := s.ring.GetInstanceState(i.lifecycler.ID); err != nil {
		return seriesOwnership{}
	}

	tokens, positions, err := s.ring.OwnedTokenPositions(i.lifecycler.ID, i.ownershipOperation())
	if err != nil {
		// Keep the previous ownership, still labelled with the view it was
		// computed from, rather than apply an empty one, which would count every
		// series as owned by nobody.
		level.Error(i.logger).Log("msg", "failed to compute owned ring token positions", "err", err)
		return s.wholeRing
	}
	if len(tokens) == 0 {
		// Instances but no tokens yet, as at startup: nothing to apply.
		return seriesOwnership{}
	}

	s.wholeRing = s.wholeRing.withBitmap(tokens, positions, source)
	s.wholeRing.viewRead = s.viewRead
	return s.wholeRing
}

// tenantOwnership returns the ownership for userID's series, given the ownership
// over the whole ring.
//
// It mirrors how the distributor picks the ring to route a tenant's writes
// within: the tenant's shuffle-shard subring when shuffle sharding applies and
// the shard is smaller than the ring, and the whole ring otherwise. A subring
// picks different replica sets from the whole ring, so for a sharded tenant
// whole-ring ownership would count the wrong series.
//
// Subring ownership is computed again whenever the ring or the tenant's shard
// size changes. The shard size matters on its own, because a larger shard
// assigns some of the tenant's series to the new members while the old ones
// still hold them.
func (i *Ingester) tenantOwnership(userID string, wholeRing seriesOwnership) seriesOwnership {
	if wholeRing.fingerprint == 0 {
		return wholeRing
	}

	s := i.ownedSeries
	previous := s.tenants[userID]
	shardSize := i.limiter.getShardSize(userID)
	if previous.shardSize > 0 && previous.shardSize != shardSize {
		// The subring cached for the previous shard size will not be used again.
		s.ring.CleanupShuffleShardCache(userID)
	}

	// This is the condition under which Ring.ShuffleShard returns the whole ring.
	if shardSize <= 0 || s.ring.InstancesCount() <= shardSize {
		return wholeRing
	}

	// The subring is cached by the ring, so this is cheap unless the ring's
	// topology changed. Its fingerprint covers only the shard's own members and
	// their states, so a state change elsewhere in the ring, such as another
	// ingester restarting, does not recompute this tenant's ownership. A topology
	// change anywhere does, but if the bitmap comes out the same the tenant's
	// series are not re-evaluated.
	//
	// ShuffleShard is part of the ReadRing interface, but OwnedTokenPositions is
	// only on *ring.Ring: adding it to the interface would break every other
	// implementation of ReadRing. Ring.ShuffleShard always returns a *ring.Ring.
	subring, ok := s.ring.ShuffleShard(userID, shardSize).(*ring.Ring)
	if !ok {
		level.Error(i.logger).Log("msg", "shuffle shard subring is not a *ring.Ring, cannot compute owned series", "user", userID)
		return seriesOwnership{}
	}

	source := subringOwnershipFingerprint(subring.OwnershipFingerprint(), shardSize)
	if source == previous.source {
		// Unchanged, so up to date as of the current view.
		previous.viewRead = s.viewRead
		s.tenants[userID] = previous
		return previous
	}

	// An ingester which is not in the tenant's shard owns none of the series it
	// holds for the tenant. If it and the distributors disagree about the shard,
	// for example while a new shard size has reached the distributors and not
	// yet this ingester, new series still arrive here, and they count as owned
	// when they are created, so the tenant's limit still applies to them.
	tokens, positions, err := subring.OwnedTokenPositions(i.lifecycler.ID, i.ownershipOperation())
	if err != nil {
		level.Error(i.logger).Log("msg", "failed to compute owned subring token positions", "user", userID, "err", err)
		return seriesOwnership{}
	}
	ownership := previous.withBitmap(tokens, positions, source)
	ownership.shardSize = shardSize
	ownership.viewRead = s.viewRead

	if ownership.fingerprint == previous.fingerprint {
		// Unchanged. Remember the new source, so it is not computed again.
		s.tenants[userID] = ownership
	}
	return ownership
}

// ownershipOperation returns the operation the distributor routes writes with, so
// that ownership agrees with it: with -distributor.extend-writes, a write is also
// sent to an extra instance while one of its replicas is not ACTIVE.
func (i *Ingester) ownershipOperation() ring.Operation {
	if i.cfg.DistributorExtendWrites {
		return ring.Write
	}
	return ring.WriteNoExtend
}

// subringOwnershipFingerprint identifies the ownership of a tenant's subring,
// from the subring's own fingerprint, which covers its members and their states,
// and the shard size it was built for.
func subringOwnershipFingerprint(ringFingerprint uint64, shardSize int) uint64 {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], ringFingerprint)
	binary.LittleEndian.PutUint64(buf[8:], uint64(shardSize))

	h := fnv.New64a()
	_, _ = h.Write(buf[:])
	if fingerprint := h.Sum64(); fingerprint != 0 {
		return fingerprint
	}
	// Zero means unknown.
	return 1
}

// recountOwnedSeriesIfChanged recounts owned series as soon as ownership may have
// changed, rather than waiting for the next updateActiveSeries. Until then, after
// a scale-up the local limit has already shrunk while the owned count still
// includes series that moved away, so the tenant can be throttled.
//
// Only tenants whose ownership changed are recounted. It is called every
// second, so that after the lifecycler reads a changed ring, or a tenant's
// shard size changes, the owned count changes together with the local limits.
func (i *Ingester) recountOwnedSeriesIfChanged() {
	s := i.ownedSeries

	wholeRing := i.ringOwnership()
	if wholeRing.fingerprint == 0 {
		// Nothing to apply. The tracker keeps what it has.
		return
	}

	type change struct {
		userDB    *userTSDB
		ownership seriesOwnership
	}
	var changes []change
	for _, userID := range i.getTSDBUsers() {
		ownership := i.tenantOwnership(userID, wholeRing)
		if ownership.fingerprint == 0 {
			// It could not be computed. Leave the tracker as it is, and try again
			// on the next check.
			continue
		}
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}
		// Compare with the tracker itself rather than with what was last applied
		// to this tenant: a TSDB closed while idle and opened again has a new
		// tracker, which has no ownership yet.
		if userDB.activeSeries.currFingerprint == ownership.fingerprint {
			continue
		}
		changes = append(changes, change{userDB: userDB, ownership: ownership})
	}
	if len(changes) == 0 {
		return
	}

	purgeTime := time.Now().Add(-i.cfg.ActiveSeriesMetricsIdleTimeout)
	for _, c := range changes {
		c.userDB.activeSeries.UpdateMetrics(purgeTime, c.ownership.tokens, c.ownership.positions, c.ownership.fingerprint, c.ownership.viewRead)
		i.metrics.ownedSeriesPerUser.WithLabelValues(c.userDB.userID).Set(float64(c.userDB.activeSeries.Owned()))
		s.tenants[c.userDB.userID] = c.ownership
	}

	i.storeInstanceOwnedSnapshot()
}

// storeInstanceOwnedSnapshot stores the instance-level owned count used by
// PreCreation's max_series check, from the tenants' current owned counts.
//
// Series in memory without a tracker entry are counted as owned, as in the
// per-tenant check: after a restart that is everything restored from the WAL,
// and in normal running it is whatever concurrent pushes have created but not
// yet recorded. Counting the gap rather than discarding the owned count keeps
// the limit safe after a restart without turning it off for the whole cycle
// whenever the snapshot lands mid-push.
func (i *Ingester) storeInstanceOwnedSnapshot() {
	// Read before counting, so that every series created from here on is added to
	// the snapshot by PreCreation. See instanceOwnedSnapshot.count.
	seriesCreated := i.TSDBState.seriesCreated.Load()

	var owned, tracked int64
	for _, userID := range i.getTSDBUsers() {
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}
		owned += int64(userDB.activeSeries.Owned())
		tracked += int64(userDB.activeSeries.Tracked())
	}

	untracked := max(0, i.TSDBState.seriesCount.Load()-tracked)
	i.TSDBState.ownedSeries.Store(&instanceOwnedSnapshot{
		owned:         owned + untracked,
		seriesCreated: seriesCreated,
	})
}

// forgetOwnedSeriesTenants drops ownership state for tenants this ingester no
// longer holds, including their cached subrings.
func (i *Ingester) forgetOwnedSeriesTenants(seen map[string]struct{}) {
	s := i.ownedSeries
	for userID := range s.tenants {
		if _, ok := seen[userID]; !ok {
			delete(s.tenants, userID)
			s.ring.CleanupShuffleShardCache(userID)
		}
	}
}

// seedOwnedSeriesFromHeads creates tracker entries for the series every tenant's
// TSDB head holds, after a restart has rebuilt the heads from the WAL. See
// ActiveSeries.SeedSeries. It runs before the ingester joins the ring, so no push
// can race with it.
func (i *Ingester) seedOwnedSeriesFromHeads(ctx context.Context) {
	// Every restored series arrived before this process started, so before any
	// ring read it makes: the first view of the ring may judge them. Using the
	// current read number instead would wait for the next read, which after a
	// crash, with the instance still ACTIVE in the ring, is the first heartbeat.
	const ringRead = 0
	// Outside the active window, since none of these series has received a sample
	// since the restart.
	notActive := time.Now().Add(-i.cfg.ActiveSeriesMetricsIdleTimeout - time.Minute).UnixNano()

	for _, userID := range i.getTSDBUsers() {
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}
		seeded, err := seedFromHead(ctx, userDB, i.cfg.DistributorShardByAllLabels, ringRead, notActive)
		if err != nil {
			// The series not seeded count as owned until head compaction, as
			// without seeding.
			level.Warn(i.logger).Log("msg", "failed to track the series restored to the head for owned series", "user", userID, "err", err)
		}
		level.Debug(i.logger).Log("msg", "tracked the series restored to the head for owned series", "user", userID, "series", seeded)
	}
}

func seedFromHead(ctx context.Context, userDB *userTSDB, shardByAllLabels bool, ringRead uint32, nanos int64) (int, error) {
	ir, err := userDB.db.Head().Index()
	if err != nil {
		return 0, err
	}
	defer ir.Close()

	name, value := index.AllPostingsKey()
	postings, err := ir.Postings(ctx, name, value)
	if err != nil {
		return 0, err
	}

	var (
		builder labels.ScratchBuilder
		seeded  int
	)
	for postings.Next() {
		ref := postings.At()
		if err := ir.Series(ref, &builder, nil); err != nil {
			// Deleted since the postings were read.
			continue
		}
		lbls := builder.Labels()
		token := seriesToken{userID: userDB.userID, labels: cortexpb.FromLabelsToLabelAdapters(lbls), shardByAllLabels: shardByAllLabels, ringRead: ringRead}
		userDB.activeSeries.SeedSeries(lbls, lbls.Hash(), uint64(ref), token, nanos)
		seeded++
	}
	return seeded, postings.Err()
}
