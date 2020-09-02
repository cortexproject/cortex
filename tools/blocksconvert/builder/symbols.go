package builder

import (
	"math"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
)

type symbolID uint32

type symbolLabel struct {
	name, value symbolID
}

type symbolsMap struct {
	symbolsMu    sync.RWMutex
	symbols      map[string]symbolID
	symbolsInv   map[symbolID]string
	lastSymbolID symbolID
}

func newSymbolsMap() *symbolsMap {
	return &symbolsMap{
		symbolsMu:  sync.RWMutex{},
		symbols:    map[string]symbolID{},
		symbolsInv: map[symbolID]string{},
	}
}

func (sm *symbolsMap) getSymbolID(symbol string) symbolID {
	sm.symbolsMu.RLock()
	id, ok := sm.symbols[symbol]
	sm.symbolsMu.RUnlock()

	if ok {
		return id
	}

	sm.symbolsMu.Lock()
	defer sm.symbolsMu.Unlock()

	id, ok = sm.symbols[symbol]
	if ok {
		return id
	}

	if sm.lastSymbolID == math.MaxUint32 {
		panic("too many symbols, cannot add new one")
	}

	sm.lastSymbolID++
	id = sm.lastSymbolID
	sm.symbols[symbol] = id
	sm.symbolsInv[id] = symbol
	return id
}

func (sm *symbolsMap) getSortedSymbols() []string {
	sm.symbolsMu.RLock()
	defer sm.symbolsMu.RUnlock()

	result := make([]string, 0, len(sm.symbols))
	for s := range sm.symbols {
		result = append(result, s)
	}
	sort.Strings(result)
	return result
}

func (sm *symbolsMap) toSymbolLabels(m labels.Labels) []symbolLabel {
	var sl = make([]symbolLabel, 0, len(m))
	for _, l := range m {
		sl = append(sl, symbolLabel{
			name:  sm.getSymbolID(l.Name),
			value: sm.getSymbolID(l.Value),
		})
	}

	return sl
}

func (sm *symbolsMap) compareSymbolLabels(a []symbolLabel, b []symbolLabel) int {
	sm.symbolsMu.RLock()
	defer sm.symbolsMu.RUnlock()

	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if a[i].name != b[i].name {
			if sm.symbolsInv[a[i].name] < sm.symbolsInv[b[i].name] {
				return -1
			}
			return 1
		}
		if a[i].value != b[i].value {
			if sm.symbolsInv[a[i].value] < sm.symbolsInv[b[i].value] {
				return -1
			}
			return 1
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

func (sm *symbolsMap) toLabels(s []symbolLabel) labels.Labels {
	sm.symbolsMu.RLock()
	defer sm.symbolsMu.RUnlock()

	m := labels.Labels{}
	for _, sl := range s {
		m = append(m, labels.Label{
			Name:  sm.symbolsInv[sl.name],
			Value: sm.symbolsInv[sl.value],
		})
	}

	return m
}
