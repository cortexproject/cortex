package ring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenNavigator_SetIngesterTokens(t *testing.T) {
	tt := []struct {
		name      string
		before    []TokenDesc
		changeID  string
		setTokens []uint32
		expect    []TokenDesc
	}{
		{
			name:      "empty navigator",
			before:    []TokenDesc{},
			changeID:  "foo",
			setTokens: []uint32{1},
			expect: []TokenDesc{
				{Ingester: "foo", Token: 1},
			},
		},

		{
			name:      "should sort tokens",
			before:    []TokenDesc{},
			changeID:  "foo",
			setTokens: []uint32{5, 1, 3},
			expect: []TokenDesc{
				{Ingester: "foo", Token: 1},
				{Ingester: "foo", Token: 3},
				{Ingester: "foo", Token: 5},
			},
		},

		{
			name: "should override tokens from same ingester",
			before: []TokenDesc{
				{Ingester: "foo", Token: 1},
				{Ingester: "foo", Token: 3},
				{Ingester: "foo", Token: 5},
			},
			changeID:  "foo",
			setTokens: []uint32{1, 9},
			expect: []TokenDesc{
				{Ingester: "foo", Token: 1},
				{Ingester: "foo", Token: 9},
			},
		},

		{
			name: "should keep tokens from other ingesters",
			before: []TokenDesc{
				{Ingester: "bar", Token: 3},
				{Ingester: "bar", Token: 5},
			},
			changeID:  "foo",
			setTokens: []uint32{1, 9},
			expect: []TokenDesc{
				{Ingester: "foo", Token: 1},
				{Ingester: "bar", Token: 3},
				{Ingester: "bar", Token: 5},
				{Ingester: "foo", Token: 9},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			navigator := TokenNavigator(tc.before)
			navigator.SetIngesterTokens(tc.changeID, tc.setTokens)
			require.Equal(t, tc.expect, []TokenDesc(navigator))
		})
	}
}

func TestTokenNavigator_InRange(t *testing.T) {
	tt := []struct {
		desc   string
		opts   RangeOptions
		expect bool
	}{
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C"}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "F"}, false},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A"}, false},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "E"}, false},

		// Inclusivity
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", IncludeFrom: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "E", IncludeTo: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", IncludeFrom: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", IncludeTo: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", IncludeFrom: true, IncludeTo: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "E", IncludeFrom: true, IncludeTo: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", IncludeFrom: true, IncludeTo: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "F", IncludeFrom: true, IncludeTo: true}, false},

		// only consider healthy tokens
		{"A B C- D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C"}, true},
		{"A B C+ D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C"}, false},
		{"A+ B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", IncludeFrom: true}, false},
		{"A+ B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", IncludeFrom: false}, false},
		{"A B C D E+", RangeOptions{Range: TokenRange{1, 5}, ID: "E", IncludeTo: true}, false},
		{"A B C D E+", RangeOptions{Range: TokenRange{1, 5}, ID: "E", IncludeTo: false}, false},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := generateRing(t, tc.desc)
			healthy := r.TokenHealthChecker(Read)
			n := r.GetNavigator()

			ok, err := n.InRange(tc.opts, healthy)
			require.NoError(t, err)
			require.Equal(t, tc.expect, ok)
		})
	}
}

func TestTokenNavigator_Predecessors(t *testing.T) {
	tt := []struct {
		desc   string
		token  string
		n      int
		expect []string
	}{
		// Simple cases
		{"A B C", "C", 0, []string{"C"}},
		{"A B C", "C", 1, []string{"B"}},
		{"A B C", "C", 2, []string{"A"}},

		// Handling duplicates
		{"A1 A2 B1 B2 B3 C1 C2 D", "D", 0, []string{"D"}},
		{"A1 A2 B1 B2 B3 C1 C2 D", "D", 1, []string{"C1", "C2"}},
		{"A1 A2 B1 B2 B3 C1 C2 D", "D", 2, []string{"B1", "B2", "B3"}},
		{"A1 A2 B1 B2 B3 C1 C2 D", "D", 3, []string{"A1", "A2"}},

		{"A1 B1 C1 A2 B2 A3 Y", "Y", 0, []string{"Y"}},
		{"A1 B1 C1 A2 B2 A3 Y", "Y", 1, []string{"A3"}},
		{"A1 B1 C1 A2 B2 A3 Y", "Y", 2, []string{"A2", "B2"}},
		{"A1 B1 C1 A2 B2 A3 Y", "Y", 3, []string{"A1", "B1", "C1"}},

		// Wrap around ring
		{"A B C", "A", 0, []string{"A"}},
		{"A B C", "A", 1, []string{"C"}},
		{"A B C", "A", 2, []string{"B"}},

		// Only consider healthy tokens
		{"A B? C- D+ E", "E", 0, []string{"E"}},
		{"A B? C- D+ E", "E", 1, []string{"C-"}},
		{"A B? C- D+ E", "E", 2, []string{"B?"}},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := generateRing(t, tc.desc)
			healthy := r.TokenHealthChecker(Read)
			n := r.GetNavigator()

			start := r.TokenDesc(t, tc.token)
			res, err := n.Predecessors(start.Token, tc.n, healthy)
			require.NoError(t, err)

			var foundTokens []string
			for _, res := range res {
				foundTokens = append(foundTokens, r.TokenName(t, res.Token))
			}

			require.Equal(t, tc.expect, foundTokens)
		})
	}
}

func TestTokenNavigator_Successor(t *testing.T) {
	tt := []struct {
		desc         string
		token        string
		n            int
		expect       string
		includeStart bool
	}{
		// Simple cases
		{"A B C", "A", 0, "A", true},
		{"A B C", "A", 1, "B", true},
		{"A B C", "A", 2, "C", true},

		// Handling duplicates
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 0, "A", true},
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 1, "B", true},
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 2, "C", true},
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 3, "D", true},

		// Duplicates ignoring own token
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 0, "A", false},
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 1, "A", false},
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 2, "B", false},
		{"A1 A2 B1 B2 B3 C1 C2 D", "A1", 3, "C", false},

		// Wrap around ring
		{"A B C", "C", 0, "C", true},
		{"A B C", "C", 1, "A", true},
		{"A B C", "C", 2, "B", true},

		// Only consider healthy tokens
		{"A B? C- D+ E", "A", 0, "A", true},
		{"A B? C- D+ E", "A", 1, "B", true},
		{"A B? C- D+ E", "A", 2, "C", true},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := generateRing(t, tc.desc)
			healthy := r.TokenHealthChecker(Read)
			n := r.GetNavigator()

			start := r.TokenDesc(t, tc.token)
			res, err := n.Neighbor(start.Token, tc.n, tc.includeStart, healthy)

			require.NoError(t, err)
			require.Equal(t, tc.expect, res.Ingester)
		})
	}
}
