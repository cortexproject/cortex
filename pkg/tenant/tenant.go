package tenant

import (
	"errors"
	"fmt"
	"sort"
)

var (
	errTenantIDTooLong = errors.New("tenant ID is too long: max 150 characters")
)

type errTenantIDUnsupportedCharacter struct {
	pos      int
	tenantID string
}

func (e *errTenantIDUnsupportedCharacter) Error() string {
	return fmt.Sprintf(
		"tenant ID '%s' contains unsupported character '%c'",
		e.tenantID,
		e.tenantID[e.pos],
	)
}

const tenantIDsLabelSeparator = "|"

// NormalizeTenantIDs is putting the IDs in a normalized form, which is sorts and de-duplicate them
func NormalizeTenantIDs(ids []string) []string {
	sort.Strings(ids)

	// de-duplicate orgIDs
	var result []string
	for pos := range ids {
		// skip if we are matching the entry before us
		if pos > 0 && ids[pos] == ids[pos-1] {
			continue
		}

		result = append(result, ids[pos])
	}
	return result
}

// ValidTenantID
func ValidTenantID(s string) error {
	// check if it contains invalid runes
	for pos, r := range s {
		if !isSupported(r) {
			return &errTenantIDUnsupportedCharacter{
				tenantID: s,
				pos:      pos,
			}
		}
	}

	if len(s) > 150 {
		return errTenantIDTooLong
	}

	return nil
}

// this checks if a rune is supported in tenant IDs (according to
// https://cortexmetrics.io/docs/guides/limitations/#tenant-id-naming)
func isSupported(c rune) bool {
	// characters
	if ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') {
		return true
	}

	// digits
	if '0' <= c && c <= '9' {
		return true
	}

	// special
	return c == '!' ||
		c == '-' ||
		c == '_' ||
		c == '.' ||
		c == '*' ||
		c == '\'' ||
		c == '(' ||
		c == ')'
}
