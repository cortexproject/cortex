package ingester

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

// matcherEqual compares two matchers for equality
func matcherEqual(a, b *labels.Matcher) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Type == b.Type && a.Name == b.Name && a.Value == b.Value
}

// matchersEqual compares two slices of matchers for equality
func matchersEqual(a, b []*labels.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !matcherEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestOptimizeMatchers(t *testing.T) {
	tests := map[string]struct {
		input          []*labels.Matcher
		expectedSelect []*labels.Matcher
		expectedLazy   []*labels.Matcher
	}{
		"single matcher returns as select": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			},
			expectedLazy: nil,
		},
		"no matchers returns empty": {
			input:          []*labels.Matcher{},
			expectedSelect: []*labels.Matcher{},
			expectedLazy:   nil,
		},
		"no optimization needed returns all as select": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expectedLazy: nil,
		},
		"no optimization needed with .* regex returns all as select": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expectedLazy: nil,
		},
		"filters out noop regex .* when optimization is needed": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
				labels.MustNewMatcher(labels.MatchRegexp, "env", ".+"),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expectedLazy: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "env", ".+"),
			},
		},
		"moves lazy matchers to lazy category": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			},
			expectedLazy: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
			},
		},
		"moves not equal empty string to lazy": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchNotEqual, "instance", ""),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			},
			expectedLazy: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "instance", ""),
			},
		},
		"moves not regex empty string to lazy": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "instance", ""),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			},
			expectedLazy: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "instance", ""),
			},
		},
		"ensures at least one select matcher": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
				labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
			},
			expectedLazy: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),
			},
		},
		"complex case with multiple matchers": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".*"), // noop
				labels.MustNewMatcher(labels.MatchRegexp, "env", ".+"),      // lazy
				labels.MustNewMatcher(labels.MatchNotEqual, "region", ""),   // lazy
				labels.MustNewMatcher(labels.MatchEqual, "version", "v1"),
			},
			expectedSelect: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchEqual, "version", "v1"),
			},
			expectedLazy: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "env", ".+"),
				labels.MustNewMatcher(labels.MatchNotEqual, "region", ""),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			selectMatchers, lazyMatchers := optimizeMatchers(testData.input)

			assert.True(t, matchersEqual(testData.expectedSelect, selectMatchers),
				"Expected select matchers: %v, got: %v", testData.expectedSelect, selectMatchers)
			assert.True(t, matchersEqual(testData.expectedLazy, lazyMatchers),
				"Expected lazy matchers: %v, got: %v", testData.expectedLazy, lazyMatchers)
		})
	}
}

func TestCanOptimizeMatchers(t *testing.T) {
	tests := map[string]struct {
		input    []*labels.Matcher
		expected bool
	}{
		"no lazy matchers returns false": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expected: false,
		},
		"has lazy matcher returns true": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
			},
			expected: true,
		},
		"has not equal empty string returns true": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchNotEqual, "instance", ""),
			},
			expected: true,
		},
		"has not regex empty string returns true": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "instance", ""),
			},
			expected: true,
		},
		"empty matchers returns false": {
			input:    []*labels.Matcher{},
			expected: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			result := canOptimizeMatchers(testData.input)
			assert.Equal(t, testData.expected, result)
		})
	}
}

func TestLabelsMatches(t *testing.T) {
	tests := map[string]struct {
		labels   labels.Labels
		matchers []*labels.Matcher
		expected bool
	}{
		"all matchers match": {
			labels: labels.FromStrings("job", "test", "instance", "server1", "env", "prod"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expected: true,
		},
		"one matcher does not match": {
			labels: labels.FromStrings("job", "test", "instance", "server1"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server2"),
			},
			expected: false,
		},
		"regex matcher matches": {
			labels: labels.FromStrings("job", "test", "instance", "server1"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server.*"),
			},
			expected: true,
		},
		"missing label matches not equal": {
			labels: labels.FromStrings("job", "test"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "instance", "server1"),
			},
			expected: true,
		},
		"missing label does not match equal": {
			labels: labels.FromStrings("job", "test"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			},
			expected: false,
		},
		"empty matchers returns true": {
			labels:   labels.FromStrings("job", "test"),
			matchers: []*labels.Matcher{},
			expected: true,
		},
		"complex case with multiple matchers": {
			labels: labels.FromStrings("job", "test", "instance", "server1", "env", "prod", "version", "v1"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server.*"),
				labels.MustNewMatcher(labels.MatchNotEqual, "env", "dev"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "version", "v2.*"),
			},
			expected: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			result := labelsMatches(testData.labels, testData.matchers)
			assert.Equal(t, testData.expected, result)
		})
	}
}

func TestLazyMatcher(t *testing.T) {
	tests := map[string]struct {
		matcher  *labels.Matcher
		expected bool
	}{
		"regex .+ is lazy": {
			matcher:  labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
			expected: true,
		},
		"not equal empty string is lazy": {
			matcher:  labels.MustNewMatcher(labels.MatchNotEqual, "instance", ""),
			expected: true,
		},
		"not regex empty string is lazy": {
			matcher:  labels.MustNewMatcher(labels.MatchNotRegexp, "instance", ""),
			expected: true,
		},
		"equal matcher is not lazy": {
			matcher:  labels.MustNewMatcher(labels.MatchEqual, "instance", "server1"),
			expected: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			result := lazyMatcher(testData.matcher)
			assert.Equal(t, testData.expected, result)
		})
	}
}
