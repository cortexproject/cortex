package cron

//go:generate ragel -G2 -Z parse.rl

import (
	"errors"
	"fmt"
	"math/bits"
	"time"
)

const magicDOW2DOM = 0x8102040810204081 // multiplying by this number converts to a dow bitmap to dom bitmap

// ParseUTC parses a cron string in UTC timezone.
func ParseUTC(s string) (Parsed, error) {
	return parse(s)
}

func (p *Parsed) yearIsZero() bool {
	return p.low|p.high|uint64(p.end) == 0
}

func (ys *Parsed) setYear(year int) {
	y := uint64(year - 1970)
	switch {
	case y > 128:
		ys.end |= 1 << (y - 128)
	case y > 64:
		ys.high |= 1 << (y - 64)
	default:
		ys.low |= 1 << y
	}
}

// this is undefined if the year is above 2070 or below 1970
func (ys *Parsed) yearIn(year int) bool {
	y := uint64(year - 1970)
	if y >= 128 {
		return ys.end&(1<<(y-128)) > 0
	}
	if y >= 64 {
		return ys.high&(1<<(y-64)) > 0
	}
	return ys.low&(1<<y) > 0
}

func isLeap(y int) bool {
	return (y&3 == 0 && y%100 != 0) || y%400 == 0
}

// wanted to keep this a private method useful for debugging
func (p *Parsed) string() string {
	return fmt.Sprintf(`s: %b
m:%16b
h:%24b
dom:%8x
dow:%8b
year:%01x%016x%16x
month:%016b`,
		p.second,
		p.minute,
		p.hour,
		p.dom,
		p.dow,
		p.end,
		p.low,
		p.high,
		p.month)
}

// to keep staticcheck happy
var _ = (&Parsed{}).string

// Parsed is a parsed cron string.  It can be used to find the next instance of a cron task.
type Parsed struct {
	// some fields are overloaded, because @every behaves very different from a normal cron string,
	// and we want to keep this struct under 64 bytes, so it fits in a cache line, on most machines
	second uint64 // also serves as the total time-like duration (hours, minutes, seconds) for every
	minute uint64 // also serves as the month count for every
	low    uint64 // also serves as the year count for every
	high   uint64
	hour   uint32
	dom    uint32 // also serves as the day count for every queries
	end    uint8  // this is here so we can support 2098 and 2099
	ldow   uint8  //lint:ignore U1000 we plan on using this field once we add L crons
	month  uint16
	ldom   uint32 //lint:ignore U1000 we plan on using this field once we add L crons
	dow    uint8
	every  bool
	//TODO(docmerlin): add location once we support location
}

func (p *Parsed) everyYear() int {
	if !p.every {
		return 0
	}
	return int(p.low) // we overload this field to also store year counts in the every case
}

func (p *Parsed) setEveryYear(d int) {
	p.low = uint64(d) // we overload this field to also store seconds for every
}

func (p *Parsed) everyMonth() int {
	if !p.every {
		return 0
	}
	return int(p.minute) // we overload this field to also store months in the every case
}

func (p *Parsed) setEveryMonth(m int) {
	p.minute = uint64(m) // we overload this field to also store seconds for every
}

func (p *Parsed) everyDay() int {
	if !p.every {
		return 0
	}
	return int(p.dom)
}

func (p *Parsed) setEveryDay(d int) {
	p.dom = uint32(d) // we overload this field to also store seconds for every
}

func (p *Parsed) everySeconds() time.Duration {
	if !p.every {
		return 0
	}
	return time.Duration(p.second) / time.Second // we overload this field to also store seconds for every
}

func (p *Parsed) addEveryDur(s time.Duration) {
	p.second += uint64(s) // we overload this field to also store time-like duration (hour minutes seconds, in nanosecond count) for every
}

func (p *Parsed) everyZero() bool {
	return p.every && (p.low == 0 && p.minute == 0 && p.second == 0 && p.dom == 0)
}

func (p *Parsed) monthIn(m time.Month) bool {
	if m < 1 || m > 12 {
		return false
	}
	m-- //to change to zero indexed month from 1 indexed month, since the formula below requires zero indexing
	return (1<<uint16(m))&p.month == (1 << uint16(m))
}

// 1 if hour is in Parsed 0 otherwise
func (p *Parsed) hourIn(d int) int {
	return int((1 << uint(d)) & p.hour >> uint(d))
}

func (p *Parsed) minuteIn(m int) bool {
	return (1<<uint64(m))&p.minute == (1 << uint64(m))
}

var maxMonthLengths = [13]uint64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31} // we wrap back around from jan to jan to make stuff easy

func (nt *Parsed) nextYear(y, m, d uint64) int {
	if y < 1969 {
		y = 1969
	}
	y++
	yBits := y - 1970
	var addToY uint64
	if yBits >= 128 {
		y += uint64(bits.TrailingZeros8(nt.end >> (yBits - 128)))
	} else if yBits >= 64 {
		addToY += uint64(bits.TrailingZeros64(nt.high >> (yBits - 64)))
		if addToY == 64 {
			addToY -= yBits - 64
			addToY += uint64(bits.TrailingZeros8(nt.end))
		}
	} else {
		addToY = uint64(bits.TrailingZeros64(nt.low >> yBits))
		var addToYHigh uint64
		if addToY == 64 {
			addToY -= yBits
			addToYHigh = uint64(bits.TrailingZeros64(nt.high))
			if addToYHigh == 128 {
				addToY += uint64(bits.TrailingZeros8(nt.end))
			}
			addToY += addToYHigh
		}
	}
	y += addToY

	if y > 2099 {
		return -1
	}
	//Feb 29th special casing
	// if we only allow feb 29th, or  then the next year must be a leap year
	// remember we zero index months here
	if m == 1 && d == 28 {
		if isLeap(int(y)) {
			return int(y)
		}
		if y%100 == 0 {
			return int(y + 4)
		}
		if y%3 != 0 { // is multiple of 4?
			y = ((y + 4) >> 2) << 2 // next multiple of 4
			if y%100 != 0 {
				return int(y)
			}
			return int(y + 4)
		}
	}

	return int(y)
}

// returns the index for the month
func (nt *Parsed) nextMonth(m, d uint64) int {
	m = uint64(bits.TrailingZeros16(nt.month>>m)) + m
	d = uint64(bits.TrailingZeros32(nt.dom>>d)) + d
	if m > 11 { // if there is no next avaliable months
		return -1
	}
	if maxMonthLengths[m] <= d {
		m++
	}
	return int(m)
}

func (nt *Parsed) nextDay(y int, m int, d uint64) int {
	firstOfMonth := time.Date(y, time.Month(m+1), 1, 0, 0, 0, 0, time.UTC) // TODO(docmerlin): location support
	days := nt.prepDays(firstOfMonth.Weekday(), m, y)
	d++
	d = uint64(bits.TrailingZeros64(days>>d)) + d
	if m >= 12 {
		return -1
	}
	if d >= uint64(maxMonthLengths[m]) {
		return -1
	}
	return int(d)
}

func (nt *Parsed) nextHour(h uint64) int {
	h++
	h = uint64(bits.TrailingZeros32(nt.hour>>h)) + h
	if h >= 24 {
		return -1
	}
	return int(h)
}

func (nt *Parsed) nextMinute(m uint64) int {
	m++
	m = uint64(bits.TrailingZeros64(nt.minute>>m)) + m
	if m >= 60 {
		return -1
	}
	return int(m)
}

func (nt *Parsed) nextSecond(s uint64) int {
	s++
	s = uint64(bits.TrailingZeros64(nt.second>>s)) + s
	if s >= 60 {
		return -1
	}
	return int(s)
}

func (nt *Parsed) valid() bool {
	return !(nt.everyZero() || (!nt.every) && (nt.minute == 0 || nt.hour == 0 || nt.dom == 0 || nt.month == 0 || nt.dow == 0 || nt.yearIsZero()))
}

// undefined for shifts larger than 7
// firstDayOfWeek is the 0 indexed day of first day of the month
// month is zero indexed instead of 1 indexed. (i.e.: Jan is month 0)
func (nt *Parsed) prepDays(firstDayOfWeek time.Weekday, month int, year int) uint64 {
	doms := uint64(1<<maxMonthLengths[month]) - 1
	if month == 1 && isLeap(year) { // 0 indexed feb
		doms = doms | (1 << 28)
	}
	return (uint64(nt.dow) & mask7) * magicDOW2DOM >> uint64(firstDayOfWeek) & (doms & uint64(nt.dom))
}

// Next returns the next time a cron task should run given a Parsed cron string.
// It will error if the Parsed is not from a zero value.
func (nt Parsed) Next(from time.Time) (time.Time, error) {
	// handle case where we have an @every query
	if nt.every {
		ts := from.AddDate(nt.everyYear(), nt.everyMonth(), nt.everyDay())
		ts = ts.Add(time.Duration(nt.everySeconds()) * time.Second)
		if !ts.After(from) {
			return time.Time{}, errors.New("next time must be later than from time")
		}
		return ts, nil
	}
	// handle the non @every case
	y, MTime, dTime := from.Date()
	d := int(dTime - 1) //time's day is 1 indexed but our day is zero indexed
	M := int(MTime - 1) //time's month is 1 indexed in time but ours is 0 indexed
	h := from.Hour()
	m := from.Minute()
	s := from.Second()

	updateHour := nt.hour&(1<<uint(h)) == 0
	updateMin := nt.minute&(1<<uint64(m)) == 0
	updateSec := nt.second&(1<<uint64(s)) == 0 || !(updateMin && updateHour)

	if updateSec {
		if s2 := nt.nextSecond(uint64(s)); s2 < 0 {
			s = bits.TrailingZeros64(nt.second) // if not found set to first second and advance minute
			updateMin = true
		} else {
			s = s2
		}
	}
	if updateMin {
		if m2 := nt.nextMinute(uint64(m)); m2 < 0 {
			m = bits.TrailingZeros64(nt.minute) // if not found set to first second and advance minute
			updateHour = true
		} else {
			m = m2
		}
		s = bits.TrailingZeros64(nt.second) // if not found set to first second and advance minute
	}
	if updateHour {
		if h2 := nt.nextHour(uint64(h)); h2 < 0 {
			h = bits.TrailingZeros32(nt.hour) // if not found set to first hour and advance the day
			d++
		} else {
			h = h2
		}
		m = bits.TrailingZeros64(nt.minute)
		s = bits.TrailingZeros64(nt.second)
	}
	weekDayOfFirst := time.Date(y, time.Month(M+1), 1, 0, 0, 0, 0, from.Location()).Weekday()

	if (d == 28 && M == 1 && !isLeap(y)) || !nt.yearIn(y) || (nt.month&(1<<uint(M)) == 0) || nt.prepDays(weekDayOfFirst, M, y)&(1<<uint(d)) == 0 {
		h = bits.TrailingZeros32(nt.hour) // if not found set to first hour and advance the day
		m = bits.TrailingZeros64(nt.minute)
		s = bits.TrailingZeros64(nt.second)
	}
	oldYear, oldMonth, oldDay := y, M, d

	for y <= 2099 {
		if nt.prepDays(weekDayOfFirst, M, y)&(1<<uint(d)) == 0 {
			if d2 := nt.nextDay(y, M, uint64(d)); d2 < 0 {
				M++
				weekDayOfFirst = time.Date(y, time.Month(M+1), 1, 0, 0, 0, 0, from.Location()).Weekday()
				d = bits.TrailingZeros64(nt.prepDays(weekDayOfFirst, M, y))
			} else {
				d = d2
			}
		}

		if nt.month&(1<<uint(M)) == 0 {
			if M2 := nt.nextMonth(uint64(M), uint64(d)); M2 < 0 {
				M = bits.TrailingZeros16(nt.month)
				y++
				weekDayOfFirst = time.Date(y, time.Month(M+1), 1, 0, 0, 0, 0, from.Location()).Weekday()
			} else {
				M = M2
			}
			d = bits.TrailingZeros64(nt.prepDays(weekDayOfFirst, M, y))
		}

		if !nt.yearIn(y) || (d == 28 && M == 1 && !isLeap(y)) {
			y2 := nt.nextYear(uint64(y), uint64(M), uint64(d))
			if y2 < 0 {
				return time.Time{}, errors.New("could not fulfil schedule due to year")
			}

			y = y2
			M = bits.TrailingZeros16(nt.month)
			weekDayOfFirst = time.Date(y, time.Month(M+1), 1, 0, 0, 0, 0, from.Location()).Weekday()
			d = bits.TrailingZeros64(nt.prepDays(weekDayOfFirst, M, y))
		}
		// if no changes, return
		if oldYear == y && oldMonth == M && oldDay == d {
			if nt.prepDays(weekDayOfFirst, M, y)&(1<<uint(d)) != 0 && (nt.yearIn(y) && (d != 28 || M != 1 || isLeap(y))) && nt.month&(1<<uint(M)) != 0 {
				return time.Date(y, time.Month(M+1), int(d+1), h, m, s, 0, from.Location()), nil // again we 0 index day of month, and the month but the actual date doesn't
			}
		}
		oldYear, oldMonth, oldDay = y, M, d
	}
	return time.Time{}, errors.New("could not fulfil schedule before 2100")
}
