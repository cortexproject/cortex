package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

// ParseTime will parse a time literal from a string.
func ParseTime(lit string) (time.Time, error) {
	if !strings.Contains(lit, "T") {
		// This is a date.
		return time.Parse("2006-01-02", lit)
	}
	// todo(jsternberg): need to also parse when there is no time offset.
	return time.Parse(time.RFC3339Nano, lit)
}

// MustParseTime parses a time literal and panics in the case of an error.
func MustParseTime(lit string) time.Time {
	ts, err := ParseTime(lit)
	if err != nil {
		panic(err)
	}
	return ts
}

// ParseDuration will convert a string into components of the duration.
func ParseDuration(lit string) ([]ast.Duration, error) {
	var values []ast.Duration
	for len(lit) > 0 {
		n := 0
		for n < len(lit) {
			ch, size := utf8.DecodeRuneInString(lit[n:])
			if size == 0 {
				panic("invalid rune in duration")
			}

			if !unicode.IsDigit(ch) {
				break
			}
			n += size
		}

		if n == 0 {
			return nil, errors.Newf(codes.Invalid, "invalid duration %s", lit)
		}

		magnitude, err := strconv.ParseInt(lit[:n], 10, 64)
		if err != nil {
			return nil, err
		}
		lit = lit[n:]

		n = 0
		for n < len(lit) {
			ch, size := utf8.DecodeRuneInString(lit[n:])
			if size == 0 {
				panic("invalid rune in duration")
			}

			if !unicode.IsLetter(ch) {
				break
			}
			n += size
		}

		if n == 0 {
			return nil, errors.Newf(codes.Invalid, "duration is missing a unit: %s", lit)
		}

		unit := lit[:n]
		if unit == "µs" {
			unit = "us"
		}
		values = append(values, ast.Duration{
			Magnitude: magnitude,
			Unit:      unit,
		})
		lit = lit[n:]
	}
	return values, nil
}

// ParseString removes quotes and unescapes the string literal.
func ParseString(lit string) (string, error) {
	if len(lit) < 2 || lit[0] != '"' || lit[len(lit)-1] != '"' {
		return "", fmt.Errorf("invalid syntax")
	}
	return ParseText(lit[1 : len(lit)-1])
}

// ParseText parses a UTF-8 block of text with escaping rules.
func ParseText(lit string) (string, error) {
	var (
		builder    strings.Builder
		width, pos int
		err        error
	)
	builder.Grow(len(lit))
	for pos < len(lit) {
		width, err = writeNextUnescapedRune(lit[pos:], &builder)
		if err != nil {
			return "", err
		}
		pos += width
	}
	return builder.String(), nil
}

// writeNextUnescapedRune writes a rune to builder from s.
// The rune is the next decoded UTF-8 rune with escaping rules applied.
func writeNextUnescapedRune(s string, builder *strings.Builder) (width int, err error) {
	var r rune
	r, width = utf8.DecodeRuneInString(s)
	if r == '\\' {
		next, w := utf8.DecodeRuneInString(s[width:])
		width += w
		switch next {
		case 'n':
			r = '\n'
		case 'r':
			r = '\r'
		case 't':
			r = '\t'
		case '\\':
			r = '\\'
		case '"':
			r = '"'
		case '$':
			r = '$'
		case 'x':
			// Decode two hex chars as a single byte
			if len(s[width:]) < 2 {
				return 0, fmt.Errorf("invalid byte value %q", s[width:])
			}
			ch1, ok1 := fromHexChar(s[width])
			ch2, ok2 := fromHexChar(s[width+1])
			if !ok1 || !ok2 {
				return 0, fmt.Errorf("invalid byte value %q", s[width:])
			}
			builder.WriteByte((ch1 << 4) | ch2)
			return width + 2, nil
		default:
			return 0, fmt.Errorf("invalid escape character %q", next)
		}
	}
	// sanity check before writing the rune
	if width > 0 {
		builder.WriteRune(r)
	}
	return
}

// fromHexChar converts a hex character into its value and a success flag.
func fromHexChar(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// ParseRegexp converts text surrounded by forward slashes into a regular expression.
func ParseRegexp(lit string) (*regexp.Regexp, error) {
	if len(lit) < 3 {
		return nil, fmt.Errorf("regexp must be at least 3 characters")
	}

	if lit[0] != '/' {
		return nil, fmt.Errorf("regexp literal must start with a slash")
	} else if lit[len(lit)-1] != '/' {
		return nil, fmt.Errorf("regexp literal must end with a slash")
	}

	expr := lit[1 : len(lit)-1]
	if index := strings.Index(expr, "\\/"); index != -1 {
		expr = strings.Replace(expr, "\\/", "/", -1)
	}
	return regexp.Compile(expr)
}
