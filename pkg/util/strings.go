package util

// StringsContain returns true if haystack contains needle.
func StringsContain(haystack []string, needle string) bool {
	for _, v := range haystack {
		if needle == v {
			return true
		}
	}

	return false
}
