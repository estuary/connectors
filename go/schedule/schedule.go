package schedule

import (
	"context"
	"fmt"
	"strings"
	"time"
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
