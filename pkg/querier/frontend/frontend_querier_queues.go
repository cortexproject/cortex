package frontend

import (
	"crypto/md5"
	"encoding/binary"
	"math/rand"
	"sort"
)

// This struct holds user queues for pending requests. It also keeps track of connected queriers,
// and mapping between users and queriers.
type queues struct {
	userQueues map[string]*userQueue

	// List of all users with queues, used for iteration when searching for next queue to handle.
	// Users removed from the middle are replaced with "". To avoid skipping users during iteration, we only shrink
	// this list when there are ""'s at the end of it.
	users []string

	maxUserQueueSize int

	// Number of connections per querier.
	querierConnections map[string]int
	// Sorted list of querier names, used when creating per-user shard.
	sortedQueriers []string
}

type userQueue struct {
	ch chan *request

	// If not nil, only these queriers can handle user requests. If nil, all queriers can.
	// We set this to nil if number of available queriers <= maxQueriers.
	queriers    map[string]struct{}
	maxQueriers int

	// Seed for shuffle sharding of queriers. This seed is based on userID only and is therefore consistent
	// between different frontends.
	seed int64

	// Points back to 'users' field in queues. Enables quick cleanup.
	index int
}

func newUserQueues(maxUserQueueSize int) *queues {
	return &queues{
		userQueues:         map[string]*userQueue{},
		users:              nil,
		maxUserQueueSize:   maxUserQueueSize,
		querierConnections: map[string]int{},
		sortedQueriers:     nil,
	}
}

func (q *queues) len() int {
	return len(q.userQueues)
}

func (q *queues) deleteQueue(userID string) {
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
func (q *queues) getOrAddQueue(userID string, maxQueriers int) chan *request {
	// Empty user is not allowed, as that would break our users list ("" is used for free spot).
	if userID == "" {
		return nil
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	uq := q.userQueues[userID]

	if uq == nil {
		uq = &userQueue{
			ch:    make(chan *request, q.maxUserQueueSize),
			seed:  getSeedForUser(userID),
			index: -1,
		}
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
	}

	if uq.maxQueriers != maxQueriers {
		uq.maxQueriers = maxQueriers
		uq.queriers = q.selectQueriersForUser(uq.seed, q.sortedQueriers, maxQueriers)
	}

	return uq.ch
}

// Finds next queue for the querier. To support fair scheduling between users, client is expected
// to pass last user index returned by this function as argument. Is there was no previous
// last user index, use -1.
func (q *queues) getNextQueueForQuerier(lastUserIndex int, querier string) (chan *request, string, int) {
	uid := lastUserIndex

	for iters := 0; iters < len(q.users); iters++ {
		uid = uid + 1

		// Don't use "mod len(q.users)", as that could skip users at the beginning of the list
		// for example when q.users has shrinked since last call.
		if uid >= len(q.users) {
			uid = 0
		}

		u := q.users[uid]
		if u == "" {
			continue
		}

		q := q.userQueues[u]

		if q.queriers != nil {
			if _, ok := q.queriers[querier]; !ok {
				// This querier is not handling the user.
				continue
			}
		}

		return q.ch, u, uid
	}
	return nil, "", uid
}

func (q *queues) addQuerierConnection(querier string) {
	conns := q.querierConnections[querier]

	q.querierConnections[querier] = conns + 1

	// First connection from this querier.
	if conns == 0 {
		q.sortedQueriers = append(q.sortedQueriers, querier)
		sort.Strings(q.sortedQueriers)

		q.recomputeUserQueriers()
	}
}

func (q *queues) removeQuerierConnection(querier string) {
	conns := q.querierConnections[querier]
	if conns <= 0 {
		return
	}

	conns--
	if conns > 0 {
		q.querierConnections[querier] = conns
	} else {
		delete(q.querierConnections, querier)

		ix := sort.SearchStrings(q.sortedQueriers, querier)
		q.sortedQueriers = append(q.sortedQueriers[:ix], q.sortedQueriers[ix+1:]...)

		q.recomputeUserQueriers()
	}
}

func (q *queues) recomputeUserQueriers() {
	for _, uq := range q.userQueues {
		uq.queriers = q.selectQueriersForUser(uq.seed, q.sortedQueriers, uq.maxQueriers)
	}
}

func (q *queues) selectQueriersForUser(userSeed int64, allSortedQueriers []string, maxQueriers int) map[string]struct{} {
	if maxQueriers == 0 || len(allSortedQueriers) <= maxQueriers {
		// All queriers can be used.
		return nil
	}

	result := make(map[string]struct{}, maxQueriers)
	rnd := rand.New(rand.NewSource(userSeed))

	for len(result) < maxQueriers {
		ix := rnd.Intn(len(allSortedQueriers))
		result[allSortedQueriers[ix]] = struct{}{}
	}

	return result
}

func getSeedForUser(user string) int64 {
	d := md5.New()
	d.Write([]byte(user))
	buf := d.Sum(nil)
	return int64(binary.BigEndian.Uint64(buf[:8]) ^ binary.BigEndian.Uint64(buf[8:]))
}
