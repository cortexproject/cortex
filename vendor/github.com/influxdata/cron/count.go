package cron

import (
	"math"
	"math/bits"
	"time"
)

const (
	sixyPositions       = uint64(0xfffffffffffffff)
	twentyFourPositions = uint32(0xffffff)
)

// Count gives us a count of the number of calls Parsed.Next would take to iterate though the time interval [from, to).
// We try to be O(1) where possible.  However for periods longer than a month that don't use @every, it is worst case
// O(m) where m is the number of months in the interval.
func (nt *Parsed) Count(from, to time.Time) (count int) {

	if nt.every {
		return nt.countEvery(from, to)
	}

	// advance to the first run on the interval [from, to)
	// this simplifies things a lot later on
	var err error
	from, err = nt.Next(from.Add(-1))
	if err != nil {
		return 0
	}
	to = to.Add(-1).Truncate(time.Second) // this is to handle the fact that to is at the open interval.
	toY, toMo, toD := to.Date()
	toD-- // zero index the day so that the first of the month is day 0, and the second is day 1 and so on.  This saves us calculations later.
	toH, toM, toS := to.Clock()
	for from.Before(to) {
		fromY, fromMo, fromD := from.Date()
		fromD-- // zero index the day.
		fromH, fromM, fromS := from.Clock()
		weekDayOfFirstOfMonth := time.Date(fromY, fromMo, 1, 0, 0, 0, 0, from.Location()).Weekday()
		days := nt.prepDays(weekDayOfFirstOfMonth, int(fromMo-1), fromY)

		// some masks
		toDMask := uint64(math.MaxUint64) >> (63 - uint64(toD))
		fromDMask := uint64(math.MaxUint64) << uint64(fromD)
		fromHMask := twentyFourPositions << uint(fromH)
		toHMask := twentyFourPositions >> (23 - uint(toH))
		fromSMask := sixyPositions << uint(fromS)
		toSMask := sixyPositions >> (59 - uint(toS))
		fromMMask := sixyPositions << uint(fromM)
		toMMask := sixyPositions >> (59 - uint(toM))

		fromDBit := uint64(1) << uint(fromD)
		if fromMo == toMo && fromY == toY {
			toMIn := int(nt.minute & (1 << uint(toM)) >> uint(toM))
			if !nt.monthIn(toMo) {
				return count
			}
			d := days & fromDMask & toDMask

			toDIn := ((int(1) << uint(toD)) & int(d)) >> uint(toD)
			// we can assume here that toS > fromS, because we know that timeRange > 0
			toHIn := nt.hourIn(toH)

			toMBit := uint64(1) << uint(toM)
			fromMBit := uint64(1) << uint(fromM)

			toHBit := uint32(1 << uint(toH))
			fromHBit := uint32(1) << uint(fromH)
			toDBit := uint64(1 << uint(toD))

			if fromD == toD {
				if toDIn == 0 {
					return count
				}

				if fromH == toH {
					if toHIn == 0 {
						return count
					}
					if fromM == toM {
						if !nt.minuteIn(toM) {
							return 0
						}
						count += bits.OnesCount64(nt.second & fromSMask & toSMask)
						return count
					}

					// we can assume here that toM > fromM and we know that fromM is in nt
					// count for first minute
					count += bits.OnesCount64(nt.second & fromSMask)
					// count for last minute
					count += bits.OnesCount64(nt.second&toSMask) * toMIn
					// count for inbetween minutes
					mCount := bits.OnesCount64(nt.minute & toMMask & fromMMask &^ fromMBit &^ toMBit)
					if mCount > 0 {
						count += bits.OnesCount64(nt.second) * mCount
					}
					count += 0
					return count
				}
				// count for first minute
				count += bits.OnesCount64(nt.second & fromSMask)
				// count for first hour except first minute
				count += (bits.OnesCount64(nt.minute&fromMMask) - 1) * bits.OnesCount64(nt.second)
				// count for the between hours
				count += bits.OnesCount64(nt.second) * bits.OnesCount64(nt.minute) * bits.OnesCount32(nt.hour&fromHMask&toHMask&^toHBit&^fromHBit)

				// count for the last minute
				count += bits.OnesCount64(nt.second&toSMask) * toMIn
				// count for last hour except last minute
				count += bits.OnesCount64(nt.minute&toMMask&^toMBit) * bits.OnesCount64(nt.second) * toHIn
				return count
			}
			// count for first minute
			count += bits.OnesCount64(nt.second & fromSMask)
			// count for first hour except first minute
			count += (bits.OnesCount64(nt.minute&fromMMask) - 1) * bits.OnesCount64(nt.second)
			// count for first day except first hour
			count += (bits.OnesCount32(nt.hour&fromHMask) - 1) * bits.OnesCount64(nt.minute) * bits.OnesCount64(nt.second)

			// count for last minute
			count += bits.OnesCount64(nt.second&toSMask) * toHIn * toDIn * toMIn
			// count for last hour except last minute
			count += bits.OnesCount64(nt.minute&toMMask&^toMBit) * bits.OnesCount64(nt.second) * toHIn * toDIn

			// count for the last day except the last hour
			count += bits.OnesCount64(nt.second) * bits.OnesCount64(nt.minute) * bits.OnesCount32(nt.hour&toHMask&^toHBit) * toDIn
			// count for the between days
			count += bits.OnesCount64(nt.second) * bits.OnesCount64(nt.minute) * bits.OnesCount32(nt.hour) * bits.OnesCount64(d&^toDBit&^fromDBit)
			return count
		}
		// count for first minute
		count += bits.OnesCount64(nt.second & fromSMask)
		// count for first hour except first minute
		count += (bits.OnesCount64(nt.minute&fromMMask) - 1) * bits.OnesCount64(nt.second)
		// count for first day except first hour
		count += (bits.OnesCount32(nt.hour&fromHMask) - 1) * bits.OnesCount64(nt.minute) * bits.OnesCount64(nt.second)
		//count for first month except first day
		count += bits.OnesCount64(days&fromDMask&^fromDBit) * bits.OnesCount64(nt.second) * bits.OnesCount64(nt.minute) * bits.OnesCount32(nt.hour)

		if fromMo == 12 {
			fromMo = time.January
			fromY++
		} else {
			fromMo++
		}

		from = time.Date(fromY, fromMo, 1, 0, 0, 0, 0, from.Location())
		from, err = nt.Next(from.Add(-1))
		if err != nil {
			return count
		}

	}

	return count
}

func (nt *Parsed) countEvery(from, to time.Time) int {
	if to.Sub(from) <= time.Second {
		return 0
	}

	fromY, fromMo, fromD := from.Date()
	fromH, fromM, fromS := from.Clock()

	d0 := nt.everyDay()
	m0 := nt.everyMonth()
	y0 := nt.everyYear()
	s := nt.everySeconds() * time.Second

	if d0 != 0 || m0 != 0 || y0 != 0 {
		// TODO(docmerlin): picking a better start would probably speed things up.
		n0, n1, n := 0, 1, 1

		// because date is a discontinuous but monotonically increasing function we can use the bisection method
		// This should be pretty fast, because the space we are working over is pretty small.
		// TODO(docmerlin): update to a method that uses slope, as this is a good candidate for it, we just need to make sure that it is one that is guaranteed to result in a stable solution for integers
		// first find an n s.t. the the time is after or equal to to
		for ; time.Date(fromY+y0*n1, fromMo+time.Month(m0*n1), fromD+d0*n1, fromH, fromM, fromS, 0, time.UTC).Add(time.Duration(n1) * time.Duration(s)).Before(to); n1 = n1 << 1 {
			// n0 is the previous n1
			n0 = n1
		}

		var t0, t1 time.Time
		t0 = time.Date(fromY+y0*n0, fromMo+time.Month(m0*n0), fromD+d0*n0, fromH, fromM, fromS, 0, time.UTC).Add(time.Duration(n0) * time.Duration(s))
		t1 = time.Date(fromY+y0*n1, fromMo+time.Month(m0*n1), fromD+d0*n1, fromH, fromM, fromS, 0, time.UTC).Add(time.Duration(n1) * time.Duration(s))

		for n1 != n0 {
			n = (n1 + n0) >> 1
			t := time.Date(fromY+y0*n, fromMo+time.Month(m0*n), fromD+d0*n, fromH, fromM, fromS, 0, time.UTC).Add(time.Duration(n) * time.Duration(s))
			if to.Sub(t0) > t1.Sub(to) {
				if t0 == t {
					n1 = n
					t1 = t
				}
				n0 = n
				t0 = t
			} else {
				if t1 == t {
					n0 = n
					t0 = t
				}
				n1 = n
				t1 = t
			}
		}
		return n
	}
	return int(to.Sub(from).Truncate(time.Second) / (time.Duration(nt.everySeconds()) * time.Second))
}
