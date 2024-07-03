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

// Validate checks whether the provided schedule description is valid and returns an error if it's incorrect.
func Validate(desc string) error {
	var _, err = Parse(desc)
	return err
}

// Parse turns a textual schedule description into an object with the Schedule interface.
func Parse(desc string) (Schedule, error) {
	if pollInterval, err := time.ParseDuration(desc); err == nil {
		return &periodicSchedule{Period: pollInterval}, nil
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
