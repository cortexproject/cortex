package util

import (
	"strings"
	"unicode"
)

// StringsContain returns true if the search value is within the list of input values.
func StringsContain(values []string, search string) bool {
	for _, v := range values {
		if search == v {
			return true
		}
	}

	return false
}

// StringsMap returns a map where keys are input values.
func StringsMap(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}

// HasPrefixAndRandomNumberOnly ensures that a name is only made of the prefix and a random number
// specifically for use with dyanmo table management
func HasPrefixAndRandomNumberOnly(fullname, prefix string) (passes bool) {
	if strings.HasPrefix(fullname, prefix) {
		suffix := strings.TrimPrefix(fullname, prefix)
		for _, s := range suffix {
			if !unicode.IsNumber(s) {
				return
			}
		}
		passes = true
	}
	return
}
