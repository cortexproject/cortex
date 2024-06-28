package ring

import (
	"container/heap"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"
)

const (
	maxTokenValue = math.MaxUint32

	minimizeSpreadTokenStrategy = "minimize-spread"
	randomTokenStrategy         = "random"
)

var (
	supportedTokenStrategy = []string{strings.ToLower(randomTokenStrategy), strings.ToLower(minimizeSpreadTokenStrategy)}
)

type TokenGenerator interface {
	// GenerateTokens make numTokens unique random tokens, none of which clash
	// with takenTokens. Generated tokens are sorted.
	// GenerateTokens can return any number of token between 0 and numTokens if force is set to false.
	// If force is set to true, all tokens needs to be generated
	GenerateTokens(ring *Desc, id, zone string, numTokens int, force bool) []uint32
}

type RandomTokenGenerator struct{}

func NewRandomTokenGenerator() TokenGenerator {
	return &RandomTokenGenerator{}
}

func (g *RandomTokenGenerator) GenerateTokens(ring *Desc, _, _ string, numTokens int, _ bool) []uint32 {
	if numTokens <= 0 {
		return []uint32{}
	}

	takenTokens := ring.GetTokens()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	used := make(map[uint32]bool, len(takenTokens))
	for _, v := range takenTokens {
		used[v] = true
	}

	tokens := make([]uint32, 0, numTokens)
	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		if used[candidate] {
			continue
		}
		used[candidate] = true
		tokens = append(tokens, candidate)
		i++
	}

	// Ensure returned tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return tokens
}

type MinimizeSpreadTokenGenerator struct {
	innerGenerator TokenGenerator
}

func NewMinimizeSpreadTokenGenerator() TokenGenerator {
	return &MinimizeSpreadTokenGenerator{
		innerGenerator: NewRandomTokenGenerator(),
	}
}

// GenerateTokens try to place nearly generated tokens on the optimal position given the existing ingesters in the ring.
// In order to do so, order all the existing ingester on the ring based on its ownership (by az), and start to create
// new tokens in order to balance out the ownership amongst all ingesters.
func (g *MinimizeSpreadTokenGenerator) GenerateTokens(ring *Desc, id, zone string, numTokens int, force bool) []uint32 {
	if numTokens <= 0 {
		return []uint32{}
	}

	r := make([]uint32, 0, numTokens)
	usedTokens := map[uint32]string{}
	instanceTokens := make([][]uint32, 0, len(ring.Ingesters))
	tokensPerInstanceWithDistance := map[string]*totalTokenPerInstance{}

	for i, instance := range ring.GetIngesters() {
		for _, token := range instance.Tokens {
			usedTokens[token] = i
		}

		// Only take in consideration tokens from instances in the same AZ
		if i != id && instance.Zone == zone {
			instanceTokens = append(instanceTokens, instance.Tokens)
			tokensPerInstanceWithDistance[i] = &totalTokenPerInstance{id: i, zone: instance.Zone}

			if len(instance.Tokens) == 0 {
				// If there is more than one ingester with no tokens, use MinimizeSpread only for the first registered ingester.
				// In case of a tie, use the ingester ID as a tiebreaker.
				if instance.RegisteredTimestamp < ring.Ingesters[id].RegisteredTimestamp ||
					(instance.RegisteredTimestamp == ring.Ingesters[id].RegisteredTimestamp && i < id) {
					if force {
						return g.innerGenerator.GenerateTokens(ring, id, zone, numTokens, true)
					} else {
						return make([]uint32, 0)
					}
				}

				continue
			}
		}
	}

	zonalTokens := MergeTokens(instanceTokens)

	// If we don't have tokens to split, lets create the tokens randomly
	if len(zonalTokens) == 0 {
		return g.innerGenerator.GenerateTokens(ring, id, zone, numTokens, true)
	}

	// Populate the map tokensPerInstanceWithDistance with the tokens and total distance of each ingester.
	// This map will be later on used to create the heap in order to take tokens from the ingesters with most distance
	for i := 1; i <= len(zonalTokens); i++ {
		index := i % len(zonalTokens)
		if id, ok := usedTokens[zonalTokens[index]]; ok {
			instanceDistance := tokensPerInstanceWithDistance[id]
			instanceDistance.tokens = append(instanceDistance.tokens, &tokenDistanceEntry{
				token:    zonalTokens[index],
				prev:     zonalTokens[i-1],
				distance: tokenDistance(zonalTokens[i-1], zonalTokens[index]),
			})
			instanceDistance.totalDistance += tokenDistance(zonalTokens[i-1], zonalTokens[index])
		}
	}

	distancesHeap := &tokenDistanceHeap{}

	for _, perInstance := range tokensPerInstanceWithDistance {
		sort.Slice(perInstance.tokens, func(i, j int) bool {
			return perInstance.tokens[i].distance > perInstance.tokens[j].distance
		})
		*distancesHeap = append(*distancesHeap, perInstance)
	}

	heap.Init(distancesHeap)

	currentInstance := &totalTokenPerInstance{id: id, zone: zone}
	expectedOwnership := float64(1) / (float64(len(tokensPerInstanceWithDistance) + 1))
	expectedOwnershipDistance := int64(expectedOwnership * maxTokenValue)

	for len(r) < numTokens {
		// If we don't have ingesters to take ownership or if the ownership was already completed we should fallback to
		// back fill the remaining tokens using the random algorithm
		if len(*distancesHeap) == 0 || currentInstance.totalDistance > expectedOwnershipDistance {
			r = append(r, g.innerGenerator.GenerateTokens(ring, id, zone, numTokens-len(r), true)...)
			break
		}

		// Calculating the expected distance per step taking in consideration the tokens already created
		expectedDistanceStep := (expectedOwnershipDistance - currentInstance.totalDistance) / int64(numTokens-len(r))

		m := heap.Pop(distancesHeap).(*totalTokenPerInstance)

		i := findFirst(len(m.tokens), func(x int) bool {
			return m.tokens[x].distance > expectedDistanceStep
		})

		if i >= len(m.tokens) {
			i = 0
		}
		tokenToSplit := m.tokens[i]
		m.tokens = append(m.tokens[:i], m.tokens[i+1:]...)

		if tokenToSplit.distance < expectedDistanceStep {
			expectedDistanceStep = tokenToSplit.distance - 1
		}

		var newToken uint32
		if int64(tokenToSplit.prev)+expectedDistanceStep > maxTokenValue {
			newToken = uint32(int64(tokenToSplit.prev) + expectedDistanceStep - maxTokenValue)
		} else {
			newToken = uint32(int64(tokenToSplit.prev) + expectedDistanceStep)
		}

		if _, ok := usedTokens[newToken]; !ok {
			usedTokens[newToken] = id
			r = append(r, newToken)

			m.totalDistance -= tokenDistance(tokenToSplit.prev, newToken)
			tokenToSplit.distance -= tokenDistance(tokenToSplit.prev, newToken)

			currentInstance.tokens = append(currentInstance.tokens, &tokenDistanceEntry{
				token:    newToken,
				prev:     tokenToSplit.prev,
				distance: tokenDistance(tokenToSplit.prev, newToken),
			})

			if m.id != id {
				currentInstance.totalDistance += tokenDistance(tokenToSplit.prev, newToken)
			}

			// We can split this token more times
			if tokenToSplit.distance > expectedDistanceStep {
				tokenToSplit.prev = newToken
				m.tokens = append(m.tokens, tokenToSplit)
			}
		}

		if len(m.tokens) > 0 {
			heap.Push(distancesHeap, m)
		}
	}

	sort.Slice(r, func(i, j int) bool {
		return r[i] < r[j]
	})

	return r
}

type tokenDistanceEntry struct {
	token, prev uint32
	distance    int64
}

type totalTokenPerInstance struct {
	id            string
	zone          string
	tokens        []*tokenDistanceEntry
	totalDistance int64
}

type tokenDistanceHeap []*totalTokenPerInstance

func (t tokenDistanceHeap) Len() int {
	return len(t)
}

func (t tokenDistanceHeap) Less(i, j int) bool {
	return t[i].totalDistance > t[j].totalDistance
}

func (t tokenDistanceHeap) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *tokenDistanceHeap) Push(x any) {
	*t = append(*t, x.(*totalTokenPerInstance))
}

func (t *tokenDistanceHeap) Pop() any {
	old := *t
	n := len(old)
	x := old[n-1]
	*t = old[0 : n-1]
	return x
}

// tokenDistance returns the distance between the given tokens from and to.
// The distance between a token and itself is the whole ring, i.e., math.MaxUint32 + 1.
func tokenDistance(from, to uint32) int64 {
	if from < to {
		return int64(to - from)
	}
	// the trailing +1 is needed to ensure that token 0 is counted
	return maxTokenValue - int64(from) + int64(to) + 1
}

func findFirst(n int, f func(int) bool) int {
	for i := 0; i < n; i++ {
		if f(i) {
			return i
		}
	}
	return n
}
