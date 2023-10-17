package cors

import (
	"strings"
)

type converter func(string) string

type wildcard struct {
	prefix string
	suffix string
}

func (w wildcard) match(s string) bool {
	return len(s) >= len(w.prefix)+len(w.suffix) && strings.HasPrefix(s, w.prefix) && strings.HasSuffix(s, w.suffix)
}

// split compounded header values ["foo, bar", "baz"] -> ["foo", "bar", "baz"]
func splitHeaderValues(values []string) []string {
	out := values
	copied := false
	for i, v := range values {
		needsSplit := strings.IndexByte(v, ',') != -1
		if !copied {
			if needsSplit {
				split := strings.Split(v, ",")
				out = make([]string, i, len(values)+len(split)-1)
				copy(out, values[:i])
				for _, s := range split {
					out = append(out, strings.TrimSpace(s))
				}
				copied = true
			}
		} else {
			if needsSplit {
				split := strings.Split(v, ",")
				for _, s := range split {
					out = append(out, strings.TrimSpace(s))
				}
			} else {
				out = append(out, v)
			}
		}
	}
	return out
}

// convert converts a list of string using the passed converter function
func convert(s []string, c converter) []string {
	out, _ := convertDidCopy(s, c)
	return out
}

// convertDidCopy is same as convert but returns true if it copied the slice
func convertDidCopy(s []string, c converter) ([]string, bool) {
	out := s
	copied := false
	for i, v := range s {
		if !copied {
			v2 := c(v)
			if v2 != v {
				out = make([]string, len(s))
				copy(out, s[:i])
				out[i] = v2
				copied = true
			}
		} else {
			out[i] = c(v)
		}
	}
	return out, copied
}
