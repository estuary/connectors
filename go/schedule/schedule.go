package schedule

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
)

// A Schedule represents a sequence of points in time when an action should be performed.
type Schedule interface {
	// Next returns the earliest instant in time greater than `afterTime` which
	// satisfies the schedule.
	Next(afterTime time.Time) time.Time
}

// Validate checks whether the provided schedule description is valid for a
// periodic or daily schedule and returns an error if it's incorrect.
func Validate(desc string) error {
	var _, err = Parse(desc)
	return err
}

// Parse turns a textual schedule description into an object with the Schedule interface.
func Parse(desc string) (Schedule, error) {
	if pollInterval, err := time.ParseDuration(desc); err == nil {
		return NewPeriodicSchedule(pollInterval), nil
	}
	if strings.HasPrefix(desc, "daily at ") {
		var timeOfDay, err = time.Parse("15:04Z", strings.TrimPrefix(desc, "daily at "))
		if err != nil {
			return nil, fmt.Errorf("invalid time %q (time of day should look like '13:00Z'): %w", timeOfDay, err)
		}
		return &dailySchedule{TimeOfDay: timeOfDay}, nil
	}
	return nil, fmt.Errorf("invalid polling schedule %q", desc)
}

type periodicSchedule struct {
	Period time.Duration
}

// NewPeriodicSchedule is used to directly construct a periodic schedule from a
// parsed duration.
func NewPeriodicSchedule(period time.Duration) Schedule {
	return &periodicSchedule{Period: period}
}

func (s *periodicSchedule) Next(after time.Time) time.Time {
	return after.Add(s.Period)
}

type dailySchedule struct {
	TimeOfDay time.Time
}

func (s *dailySchedule) Next(after time.Time) time.Time {
	// Construct a timestamp with the appropriate time of day, on the same day as the
	// 'after' timestamp. Then increment it day by day until it's actually greater than
	// the 'after' timestamp.
	var yyyy, mm, dd = after.UTC().Date()
	var t = time.Date(yyyy, mm, dd, s.TimeOfDay.Hour(), s.TimeOfDay.Minute(), s.TimeOfDay.Second(), 0, time.UTC)
	if !t.After(after) {
		t = t.AddDate(0, 0, 1)
	}
	return t
}

type fixedSchedule struct {
	period time.Duration
	jitter time.Duration
}

// NewFixedSchedule creates a schedule having "Next" values that occur at
// predictable points in time. For example, with a 30 minute period they will
// always be at times like X:00 and X:30. Periods larger than an hour work
// similarly, and they are predictable with respect to the start of a 24-hour
// day, so a 3 hour period would have "Next" values at 00:00, 03:00, 06:00, etc.
// An optional value for `seed` can be provided, from which a jitter will be
// calculated as a random offset (max 24 hrs) from the Unix epoch.
func NewFixedSchedule(duration string, seed []byte) (Schedule, error) {
	return newFixedSchedule(duration, seed)
}

// Internal constructor used by alternatingSchedule to directly get a typed
// fixedSchedule rather than a Schedule interface.
func newFixedSchedule(duration string, seed []byte) (*fixedSchedule, error) {
	period, err := time.ParseDuration(duration)
	if err != nil {
		return nil, fmt.Errorf("parsing interval %q: %w", duration, err)
	}

	// Hash the seed bytes and scale the result to a positive 64-bit integer.
	// This is used to come up with an offset within the range of a 24 hour day,
	// which should work for just about any reasonable duration.
	jitter := time.Duration(int64(xxhash.Sum64(seed)>>1)) % (time.Hour * 24)
	if seed == nil {
		// Convenience for no jitter - useful for tests.
		jitter = 0
	}

	return &fixedSchedule{period: period, jitter: jitter}, nil
}

func (s *fixedSchedule) Next(after time.Time) time.Time {
	if s.period == 0 {
		return after
	}

	// How many periods have fully elapsed since the epoch, offset by the jitter?
	elapsedPeriods := (after.UnixNano() - s.jitter.Nanoseconds()) / s.period.Nanoseconds()

	// The next time should happen after one additional period, again taking into
	// account the jitter.
	nextTime := (elapsedPeriods+1)*s.period.Nanoseconds() + s.jitter.Nanoseconds()

	return time.Unix(0, nextTime)
}

// clockTime represents a time of day with hour and minute precision for a
// specific location.
type clockTime struct {
	h   int
	m   int
	loc *time.Location
}

func newClockTime(ts string, loc *time.Location) (clockTime, error) {
	t, err := time.ParseInLocation("15:04", ts, loc)
	if err != nil {
		return clockTime{}, fmt.Errorf("could not parse %q as clock time: %w", ts, err)
	}

	return clockTime{
		h:   t.Hour(),
		m:   t.Minute(),
		loc: loc,
	}, nil
}

// Produces a time.Time on the same date as `dt`, but at the hour and minute of
// the clockTime.
func (ct clockTime) on(dt time.Time) time.Time {
	yyyy, mm, dd := dt.In(ct.loc).Date()
	return time.Date(yyyy, mm, dd, ct.h, ct.m, 0, 0, ct.loc)
}

// Produces a time.Time representing the next instant after dt having the
// clockTime of `ct`.
func (ct clockTime) next(dt time.Time) time.Time {
	t := ct.on(dt)
	if t.After(dt) {
		return t
	}
	return t.AddDate(0, 0, 1)
}

// Determine if the instant represented by datetime `dt` falls between the
// clockTimes `ct` and `end`, inclusive of `ct` and exclusive of `end`.
func (ct clockTime) between(dt time.Time, end clockTime) bool {
	clockStart := ct.on(dt)
	clockEnd := end.on(dt)

	if clockStart.After(clockEnd) {
		// Start and end times straddle midnight. Example: `ct` is 22:00 and
		// `end` is 02:00.
		return !clockStart.After(dt) || clockEnd.After(dt)
	}

	return !dt.Before(clockStart) && dt.Before(clockEnd)
}

type alternatingSchedule struct {
	def           fixedSchedule
	alt           fixedSchedule
	altStart      clockTime
	altEnd        clockTime
	altActiveDays [7]bool
	loc           *time.Location
}

// NewAlternatingSchedule creates a new Schedule from the provided strings and
// jitter. A `nil` jitter will result in there being no jitter. The `Next`
// values produced by this schedule use a fixedSchedule. The defaultInterval and
// alternateInterval must be Go duration strings, and alternateStart and
// alternateEnd must be in the form of HH:MM with no timezone. Timezone location
// information is provided by the location string, which must parse according to
// ParseTimezone. alternateActiveDays must parse via parseActiveDays and should
// look like "M-F" or "Su-T,Th,S" for a range of days and specific days, where
// Sunday is "Su" and Thursday is "Th" and all other days are the first letter
// of the day.
func NewAlternatingSchedule(
	defaultInterval string,
	alternateInterval string,
	alternateStart string,
	alternateEnd string,
	alternateActiveDays string,
	location string,
	jitter []byte,
) (Schedule, error) {
	if alternateStart == alternateEnd {
		return nil, fmt.Errorf("alternateStart and alternateEnd must be different: got %q and %q", alternateStart, alternateEnd)
	}

	defSched, err := newFixedSchedule(defaultInterval, jitter)
	if err != nil {
		return nil, fmt.Errorf("building default schedule: %w", err)
	}

	altSched, err := newFixedSchedule(alternateInterval, jitter)
	if err != nil {
		return nil, fmt.Errorf("building alternate schedule: %w", err)
	}

	loc, err := ParseTimezone(location)
	if err != nil {
		return nil, err
	}

	altStart, err := newClockTime(alternateStart, loc)
	if err != nil {
		return nil, fmt.Errorf("invalid alternate start time %q (time of day should look like '13:00'): %w", alternateStart, err)
	}

	altEnd, err := newClockTime(alternateEnd, loc)
	if err != nil {
		return nil, fmt.Errorf("invalid alternate end time %q (time of day should look like '13:00'): %w", alternateEnd, err)
	}

	altActiveDays, err := parseActiveDays(alternateActiveDays)
	if err != nil {
		return nil, fmt.Errorf("parsing alternate active days: %w", err)
	}

	return &alternatingSchedule{
		def:           *defSched,
		alt:           *altSched,
		altStart:      altStart,
		altEnd:        altEnd,
		altActiveDays: altActiveDays,
		loc:           loc,
	}, nil
}

// Next produces a Time object for the next scheduled time.
//
// If `after` is within the interval specified by `alternateStart` and
// `alternateEnd`, the `alternateInterval` is used to determine the next time.
// Otherwise, the `defaultInterval` is used.
//
// If the next scheduled time would "cross over" into a different scheduling
// period and that different scheduling period has a shorter interval, the next
// time is the start of the next period. But if the scheduled time crosses into
// a different scheduling period with a longer period, the next scheduled time
// is used as-is.
func (s *alternatingSchedule) Next(after time.Time) time.Time {
	this := s.def
	other := s.alt
	bound := s.nextStartOfAlt(after)

	if s.altActive(after) {
		this = s.alt
		other = s.def
		bound = s.nextEndOfAlt(after)
	}

	n := this.Next(after)

	if !n.Before(bound) && other.period < this.period {
		n = bound
	}

	return n
}

// altActive is a helper for determining if the "alternate" scheduling period is
// active for a datetime, taking into account the days of the week that the
// alternate schedule applies.
func (s *alternatingSchedule) altActive(dt time.Time) bool {
	return s.altActiveDays[dt.In(s.loc).Weekday()] &&
		s.altStart.between(dt, s.altEnd)
}

// nextStartOfAlt calculates the next time the alternate scheduling period
// begins relative to `dt`. In the most simple case, it's just the next starting
// datetime of `altStart`. But things are more complicated when considering that
// some days of the week might not be "active" for the alternate scheduling
// period.
func (s *alternatingSchedule) nextStartOfAlt(dt time.Time) time.Time {
	for i := 0; i < 7; i++ {
		today := dt.AddDate(0, 0, i)
		tomorrow := today.AddDate(0, 0, 1)

		windowStart := s.altStart.on(today)
		if windowStart.Before(today) {
			// Already past the start time for the alternate period today, so
			// the normal window for it could start tomorrow at this time.
			windowStart = s.altStart.on(tomorrow)
		}

		// The alternate window may become active if the alternate period
		// straddles midnight and today was not an active day, but tomorrow is.
		midnightTomorrow := atMidnight(tomorrow)

		if s.altActive(midnightTomorrow) && (midnightTomorrow.Before(windowStart) || !s.altActive(windowStart)) {
			return midnightTomorrow
		} else if s.altActive(windowStart) {
			return windowStart
		}
	}

	panic("nextStartOfAlt application error: at least one day of the week must be active")
}

// nextEndOfAlt calculates the next time the alternate scheduling period _ends_
// relative to `dt`. It's a little simpler than finding the start of the
// alternate scheduling period since the default schedule is active on all days.
func (s *alternatingSchedule) nextEndOfAlt(dt time.Time) time.Time {
	windowEnd := s.altEnd.next(dt)
	midnightTomorrow := atMidnight(dt.AddDate(0, 0, 1))

	if !s.altActive(midnightTomorrow) && midnightTomorrow.Before(windowEnd) {
		// Tomorrow might not be an active day for the alternate schedule, so
		// the boundary could be right at the start of the next day.
		return midnightTomorrow
	}

	return windowEnd
}

func atMidnight(dt time.Time) time.Time {
	yyyy, mm, dd := dt.UTC().Date()
	return time.Date(yyyy, mm, dd, 0, 0, 0, 0, time.UTC)
}

var (
	days = map[string]time.Weekday{
		"Su": time.Sunday,
		"M":  time.Monday,
		"T":  time.Tuesday,
		"W":  time.Wednesday,
		"Th": time.Thursday,
		"F":  time.Friday,
		"S":  time.Saturday,
	}
)

func parseActiveDays(in string) ([7]bool, error) {
	out := [7]bool{}
	lastSeen := time.Weekday(-1)

	if in == "" {
		// Special case of no days set, which means all days are active.
		for i := range out {
			out[i] = true
		}
		return out, nil
	}

	getDayCode := func(day string) (time.Weekday, error) {
		if code, ok := days[day]; ok {
			return code, nil
		}
		return -1, fmt.Errorf("invalid day specification: %q", day)
	}

	for _, parts := range strings.Split(in, ",") {
		dayRange := strings.Split(parts, "-")
		switch len(dayRange) {
		case 1:
			// Just a single day.
			if day, err := getDayCode(dayRange[0]); err != nil {
				return out, err
			} else if day <= lastSeen {
				return out, fmt.Errorf("days specified in %q must be in ascending order and not repeat", in)
			} else {
				out[day] = true
				lastSeen = day
			}

		case 2:
			// A range of days, separated by a hyphen.
			if startCode, err := getDayCode(dayRange[0]); err != nil {
				return out, err
			} else if endCode, err := getDayCode(dayRange[1]); err != nil {
				return out, err
			} else if startCode >= endCode {
				return out, fmt.Errorf("invalid day specification: %q (days of a range must be different and in increasing order)", parts)
			} else if startCode <= lastSeen {
				return out, fmt.Errorf("days specified in %q must be in ascending order and not repeat", in)
			} else {
				for code := startCode; code <= endCode; code++ {
					out[code] = true
					lastSeen = code
				}
			}

		default:
			return out, fmt.Errorf("invalid day specification: %q", parts)
		}
	}

	return out, nil
}

// WaitForNext sleeps until the next scheduled execution time, or until the
// context is cancelled.
func WaitForNext(ctx context.Context, s Schedule, after time.Time) error {
	var d = time.Until(s.Next(after))
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
