package queue

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Limits needed for the Query Scheduler - interface used for decoupling.
type Limits interface {
	// MaxOutstandingPerTenant returns the limit to the maximum number
	// of outstanding requests per tenant per request queue.
	MaxOutstandingPerTenant(user string) int

	// QueryPriority returns query priority config for the tenant, including priority level,
	// their attributes, and how many reserved queriers each priority has.
	QueryPriority(user string) validation.QueryPriority
}

// querier holds information about a querier registered in the queue.
type querier struct {
	// Number of active connections.
	connections int

	// True if the querier notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
}

// This struct holds user queues for pending requests. It also keeps track of connected queriers,
// and mapping between users and queriers.
type queues struct {
	userQueues   map[string]*userQueue
	userQueuesMx sync.RWMutex

	// List of all users with queues, used for iteration when searching for next queue to handle.
	// Users removed from the middle are replaced with "". To avoid skipping users during iteration, we only shrink
	// this list when there are ""'s at the end of it.
	users []string

	// How long to wait before removing a querier which has got disconnected
	// but hasn't notified about a graceful shutdown.
	forgetDelay time.Duration

	// Tracks queriers registered to the queue.
	queriers map[string]*querier

	// Sorted list of querier names, used when creating per-user shard.
	sortedQueriers []string

	limits Limits

	queueLength *prometheus.GaugeVec // Per user, type and priority.
}

type userQueue struct {
	queue userRequestQueue

	// If not nil, only these queriers can handle user requests. If nil, all queriers can.
	// We set this to nil if number of available queriers <= maxQueriers.
	queriers map[string]struct{}

	// Contains assigned priority for querier ID
	reservedQueriers map[string]int64

	// Stores last limit config for the user. When changed, re-populate queriers and reservedQueriers
	maxQueriers     int
	maxOutstanding  int
	priorityList    []int64
	priorityEnabled bool

	// Seed for shuffle sharding of queriers. This seed is based on userID only and is therefore consistent
	// between different frontends.
	seed int64

	// Points back to 'users' field in queues. Enables quick cleanup.
	index int
}

func newUserQueues(forgetDelay time.Duration, limits Limits, queueLength *prometheus.GaugeVec) *queues {
	return &queues{
		userQueues:     map[string]*userQueue{},
		users:          nil,
		forgetDelay:    forgetDelay,
		queriers:       map[string]*querier{},
		sortedQueriers: nil,
		limits:         limits,
		queueLength:    queueLength,
	}
}

func (q *queues) len() int {
	return len(q.userQueues)
}

func (q *queues) deleteQueue(userID string) {
	q.userQueuesMx.Lock()
	defer q.userQueuesMx.Unlock()

	uq := q.userQueues[userID]
	if uq == nil {
		return
	}

	delete(q.userQueues, userID)
	q.users[uq.index] = ""

	// Shrink users list size if possible. This is safe, and no users will be skipped during iteration.
	for ix := len(q.users) - 1; ix >= 0 && q.users[ix] == ""; ix-- {
		q.users = q.users[:ix]
	}
}

// Returns existing or new queue for user.
// MaxQueriers is used to compute which queriers should handle requests for this user.
// If maxQueriers is <= 0, all queriers can handle this user's requests.
// If maxQueriers has changed since the last call, queriers for this are recomputed.
// It's also responsible to store user configs and update the attributes if related configs have changed.
func (q *queues) getOrAddQueue(userID string, maxQueriers int) userRequestQueue {
	// Empty user is not allowed, as that would break our users list ("" is used for free spot).
	if userID == "" {
		return nil
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	q.userQueuesMx.Lock()
	defer q.userQueuesMx.Unlock()

	uq := q.userQueues[userID]
	priorityEnabled := q.limits.QueryPriority(userID).Enabled
	maxOutstanding := q.limits.MaxOutstandingPerTenant(userID)
	priorityList := getPriorityList(q.limits.QueryPriority(userID), maxQueriers)

	if uq == nil {
		uq = &userQueue{
			seed:  util.ShuffleShardSeed(userID, ""),
			index: -1,
		}

		uq.queue = q.createUserRequestQueue(userID)
		uq.maxOutstanding = q.limits.MaxOutstandingPerTenant(userID)
		q.userQueues[userID] = uq

		// Add user to the list of users... find first free spot, and put it there.
		for ix, u := range q.users {
			if u == "" {
				uq.index = ix
				q.users[ix] = userID
				break
			}
		}

		// ... or add to the end.
		if uq.index < 0 {
			uq.index = len(q.users)
			q.users = append(q.users, userID)
		}
	} else if (uq.priorityEnabled != priorityEnabled) || (!priorityEnabled && uq.maxOutstanding != maxOutstanding) {
		tmpQueue := q.createUserRequestQueue(userID)

		// flush to new queue
		for uq.queue.length() > 0 {
			tmpQueue.enqueueRequest(uq.queue.dequeueRequest(0, false))
		}

		uq.queue = tmpQueue
		uq.maxOutstanding = q.limits.MaxOutstandingPerTenant(userID)
		uq.priorityEnabled = priorityEnabled
	}

	if uq.maxQueriers != maxQueriers {
		uq.maxQueriers = maxQueriers
		uq.queriers = shuffleQueriersForUser(uq.seed, maxQueriers, q.sortedQueriers, nil)
	}

	if priorityEnabled && hasPriorityListChanged(uq.priorityList, priorityList) {
		reservedQueriers := make(map[string]int64)

		i := 0
		for _, querierID := range q.sortedQueriers {
			if i == len(priorityList) {
				break
			}
			if _, ok := uq.queriers[querierID]; ok || uq.queriers == nil {
				reservedQueriers[querierID] = priorityList[i]
				i++
			}
		}

		uq.reservedQueriers = reservedQueriers
		uq.priorityList = priorityList
		uq.priorityEnabled = priorityEnabled
	}

	return uq.queue
}

func (q *queues) createUserRequestQueue(userID string) userRequestQueue {
	if q.limits.QueryPriority(userID).Enabled {
		return NewPriorityRequestQueue(util.NewPriorityQueue(nil), userID, q.queueLength)
	}

	queueSize := q.limits.MaxOutstandingPerTenant(userID)

	return NewFIFORequestQueue(make(chan Request, queueSize), userID, q.queueLength)
}

// Finds next queue for the querier. To support fair scheduling between users, client is expected
// to pass last user index returned by this function as argument. Is there was no previous
// last user index, use -1.
func (q *queues) getNextQueueForQuerier(lastUserIndex int, querierID string) (userRequestQueue, string, int) {
	uid := lastUserIndex

	for iters := 0; iters < len(q.users); iters++ {
		uid = uid + 1

		// Don't use "mod len(q.users)", as that could skip users at the beginning of the list
		// for example when q.users has shrunk since last call.
		if uid >= len(q.users) {
			uid = 0
		}

		u := q.users[uid]
		if u == "" {
			continue
		}

		q.userQueuesMx.RLock()
		defer q.userQueuesMx.RUnlock()

		uq := q.userQueues[u]

		if uq.queriers != nil {
			if _, ok := uq.queriers[querierID]; !ok {
				// This querier is not handling the user.
				continue
			}
		}

		return uq.queue, u, uid
	}
	return nil, "", uid
}

func (q *queues) addQuerierConnection(querierID string) {
	info := q.queriers[querierID]
	if info != nil {
		info.connections++

		// Reset in case the querier re-connected while it was in the forget waiting period.
		info.shuttingDown = false
		info.disconnectedAt = time.Time{}

		return
	}

	// First connection from this querier.
	q.queriers[querierID] = &querier{connections: 1}
	q.sortedQueriers = append(q.sortedQueriers, querierID)
	sort.Strings(q.sortedQueriers)

	q.recomputeUserQueriers()
}

func (q *queues) removeQuerierConnection(querierID string, now time.Time) {
	info := q.queriers[querierID]
	if info == nil || info.connections <= 0 {
		panic("unexpected number of connections for querier")
	}

	// Decrease the number of active connections.
	info.connections--
	if info.connections > 0 {
		return
	}

	// There no more active connections. If the forget delay is configured then
	// we can remove it only if querier has announced a graceful shutdown.
	if info.shuttingDown || q.forgetDelay == 0 {
		q.removeQuerier(querierID)
		return
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	info.disconnectedAt = now
}

func (q *queues) removeQuerier(querierID string) {
	delete(q.queriers, querierID)

	ix := sort.SearchStrings(q.sortedQueriers, querierID)
	if ix >= len(q.sortedQueriers) || q.sortedQueriers[ix] != querierID {
		panic("incorrect state of sorted queriers")
	}

	q.sortedQueriers = append(q.sortedQueriers[:ix], q.sortedQueriers[ix+1:]...)

	q.recomputeUserQueriers()
}

// notifyQuerierShutdown records that a querier has sent notification about a graceful shutdown.
func (q *queues) notifyQuerierShutdown(querierID string) {
	info := q.queriers[querierID]
	if info == nil {
		// The querier may have already been removed, so we just ignore it.
		return
	}

	// If there are no more connections, we should remove the querier.
	if info.connections == 0 {
		q.removeQuerier(querierID)
		return
	}

	// Otherwise we should annotate we received a graceful shutdown notification
	// and the querier will be removed once all connections are unregistered.
	info.shuttingDown = true
}

// forgetDisconnectedQueriers removes all disconnected queriers that have gone since at least
// the forget delay. Returns the number of forgotten queriers.
func (q *queues) forgetDisconnectedQueriers(now time.Time) int {
	// Nothing to do if the forget delay is disabled.
	if q.forgetDelay == 0 {
		return 0
	}

	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-q.forgetDelay)
	forgotten := 0

	for querierID := range q.queriers {
		if info := q.queriers[querierID]; info.connections == 0 && info.disconnectedAt.Before(threshold) {
			q.removeQuerier(querierID)
			forgotten++
		}
	}

	return forgotten
}

func (q *queues) recomputeUserQueriers() {
	scratchpad := make([]string, 0, len(q.sortedQueriers))

	for _, uq := range q.userQueues {
		uq.queriers = shuffleQueriersForUser(uq.seed, uq.maxQueriers, q.sortedQueriers, scratchpad)
	}
}

// shuffleQueriersForUser returns nil if queriersToSelect is 0 or there are not enough queriers to select from.
// In that case *all* queriers should be used.
// Scratchpad is used for shuffling, to avoid new allocations. If nil, new slice is allocated.
func shuffleQueriersForUser(userSeed int64, queriersToSelect int, allSortedQueriers []string, scratchpad []string) map[string]struct{} {
	if queriersToSelect == 0 || len(allSortedQueriers) <= queriersToSelect {
		return nil
	}

	queriers := make(map[string]struct{}, queriersToSelect)
	rnd := rand.New(rand.NewSource(userSeed))

	scratchpad = scratchpad[:0]
	scratchpad = append(scratchpad, allSortedQueriers...)

	last := len(scratchpad) - 1
	for i := 0; i < queriersToSelect; i++ {
		r := rnd.Intn(last + 1)
		queriers[scratchpad[r]] = struct{}{}
		scratchpad[r], scratchpad[last] = scratchpad[last], scratchpad[r]
		last--
	}

	return queriers
}

// getPriorityList returns a list of priorities, each priority repeated as much as number of reserved queriers.
// This is used when creating map of reserved queriers.
func getPriorityList(queryPriority validation.QueryPriority, totalQuerierCount int) []int64 {
	var priorityList []int64

	if queryPriority.Enabled {
		for _, priority := range queryPriority.Priorities {
			reservedQuerierShardSize := util.DynamicShardSize(priority.ReservedQueriers, totalQuerierCount)

			for i := 0; i < reservedQuerierShardSize; i++ {
				priorityList = append(priorityList, priority.Priority)
			}
		}
	}

	if len(priorityList) > totalQuerierCount {
		return []int64{}
	}

	return priorityList
}

func hasPriorityListChanged(old, new []int64) bool {
	if len(old) != len(new) {
		return true
	}
	for i := range old {
		if old[i] != new[i] {
			return true
		}
	}
	return false
}

// MockLimits implements the Limits interface. Used in tests only.
type MockLimits struct {
	MaxOutstanding        int
	MaxQueriersPerUserVal float64
	QueryPriorityVal      validation.QueryPriority
}

func (l MockLimits) MaxQueriersPerUser(_ string) float64 {
	return l.MaxQueriersPerUserVal
}

func (l MockLimits) MaxOutstandingPerTenant(_ string) int {
	return l.MaxOutstanding
}

func (l MockLimits) QueryPriority(_ string) validation.QueryPriority {
	return l.QueryPriorityVal
}
