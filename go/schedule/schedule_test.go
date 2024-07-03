package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPeriodicSchedule(t *testing.T) {
	for _, tc := range []struct {
		Schedule string
		After    string
		Expect   string
	}{
		{"1h", "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"2h", "2024-02-15T12:34:56Z", "2024-02-15T14:34:56Z"},
		{"6h", "2024-02-15T19:34:56Z", "2024-02-16T01:34:56Z"},
		{"24h", "2024-02-15T19:34:56Z", "2024-02-16T19:34:56Z"},
		{"168h", "2024-02-15T19:34:56Z", "2024-02-22T19:34:56Z"},
		{"30m", "2024-02-15T19:34:56Z", "2024-02-15T20:04:56Z"},
		{"10m", "2024-02-15T19:34:56Z", "2024-02-15T19:44:56Z"},
		{"1m", "2024-02-15T19:34:56Z", "2024-02-15T19:35:56Z"},
		{"30s", "2024-02-15T19:34:56Z", "2024-02-15T19:35:26Z"},
		{"10s", "2024-02-15T19:34:56Z", "2024-02-15T19:35:06Z"},
		{"1s", "2024-02-15T19:34:56Z", "2024-02-15T19:34:57Z"},
	} {
		sched, err := Parse(tc.Schedule)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339, tc.After)
		require.NoError(t, err)
		var ts = sched.Next(after)
		require.Equal(t, tc.Expect, ts.Format(time.RFC3339))
	}
}

func TestDailySchedule(t *testing.T) {
	for _, tc := range []struct {
		Schedule string
		After    string
		Expect   string
	}{
		{"daily at 6:00Z", "2024-02-15T01:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 6:00Z", "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 6:00Z", "2024-02-15T06:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 6:00Z", "2024-02-15T07:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T01:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T06:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T07:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 13:55Z", "2024-02-15T01:00:00Z", "2024-02-15T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T13:54:00Z", "2024-02-15T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T13:55:00Z", "2024-02-16T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T13:56:00Z", "2024-02-16T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T23:00:00Z", "2024-02-16T13:55:00Z"},
	} {
		sched, err := Parse(tc.Schedule)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339, tc.After)
		require.NoError(t, err)
		var ts = sched.Next(after)
		require.Equal(t, tc.Expect, ts.Format(time.RFC3339))
	}
}

func TestFixedSchedule(t *testing.T) {
	for _, tc := range []struct {
		Schedule string
		Seed     []byte
		After    string
		Expect   string
	}{
		{"1h", nil, "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"2h", nil, "2024-02-15T12:34:56Z", "2024-02-15T14:00:00Z"},
		{"6h", nil, "2024-02-15T19:34:56Z", "2024-02-16T00:00:00Z"},
		{"24h", nil, "2024-02-15T19:34:56Z", "2024-02-16T00:00:00Z"},
		{"168h", nil, "2024-02-15T19:34:56Z", "2024-02-22T00:00:00Z"},
		{"336h", nil, "2024-02-15T19:34:56Z", "2024-02-29T00:00:00Z"},
		{"30m", nil, "2024-02-15T19:34:56Z", "2024-02-15T20:00:00Z"},
		{"10m", nil, "2024-02-15T19:34:56Z", "2024-02-15T19:40:00Z"},
		{"1m", nil, "2024-02-15T19:34:56Z", "2024-02-15T19:35:00Z"},
		{"30s", nil, "2024-02-15T19:34:56Z", "2024-02-15T19:35:00Z"},
		{"10s", nil, "2024-02-15T19:34:56Z", "2024-02-15T19:35:00Z"},
		{"1h", nil, "2024-02-15T01:01:00Z", "2024-02-15T02:00:00Z"},
		{"1h", nil, "2024-02-15T01:30:00Z", "2024-02-15T02:00:00Z"},
		{"1h", nil, "2024-02-15T01:59:00Z", "2024-02-15T02:00:00Z"},

		{"1h", []byte("jitterbytes"), "2024-02-15T01:59:00Z", "2024-02-15T02:03:23Z"},
		{"1h", []byte("jitterbytes"), "2024-02-15T01:59:30Z", "2024-02-15T02:03:23Z"},
		{"1h", []byte("jitterbytes"), "2024-02-15T02:03:22Z", "2024-02-15T02:03:23Z"},
		{"1h", []byte("jitterbytes"), "2024-02-15T02:03:23.172477884Z", "2024-02-15T02:03:23Z"},
		{"1h", []byte("jitterbytes"), "2024-02-15T02:03:23.172477885Z", "2024-02-15T03:03:23Z"},
		{"1h", []byte("jitterbytes"), "2024-02-15T02:03:23.172477886Z", "2024-02-15T03:03:23Z"},
		{"30s", []byte("jitterbytes"), "2024-02-15T02:03:22Z", "2024-02-15T02:03:23Z"},
		{"30s", []byte("jitterbytes"), "2024-02-15T02:03:24Z", "2024-02-15T02:03:53Z"},
	} {
		sched, err := newFixedSchedule(tc.Schedule, tc.Seed)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339Nano, tc.After)
		require.NoError(t, err)
		var ts = sched.Next(after)
		require.Equal(t, tc.Expect, ts.UTC().Format(time.RFC3339))
	}
}

func TestAlternatingSchedule(t *testing.T) {
	t.Run("no active day set", func(t *testing.T) {
		for _, tc := range []struct {
			DefaultInterval   string
			AlternateInterval string
			AlternateStart    string
			AlternateEnd      string
			After             string
			Expect            string
		}{
			// Typical cases.
			{"4h", "5m", "8:00", "17:00", "2024-02-15T00:00:00Z", "2024-02-15T04:00:00Z"},
			{"4h", "5m", "8:00", "17:00", "2024-02-15T01:00:00Z", "2024-02-15T04:00:00Z"}, // fixed scheduling effect for 4 hour interval
			{"4h", "5m", "8:00", "17:00", "2024-02-15T08:00:00Z", "2024-02-15T08:05:00Z"},
			{"4h", "5m", "8:00", "17:00", "2024-02-15T17:00:00Z", "2024-02-15T20:00:00Z"},
			{"4h", "5m", "8:00", "17:00", "2024-02-15T07:59:59Z", "2024-02-15T08:00:00Z"}, // crosses boundary into lower interval
			{"4h", "5m", "8:00", "17:00", "2024-02-15T16:59:59Z", "2024-02-15T17:00:00Z"},
			{"4h", "0s", "8:00", "17:00", "2024-02-15T16:59:59Z", "2024-02-15T16:59:59Z"}, // no delay configured

			// Alternate interval is larger than the default interval.
			{"5m", "15m", "8:00", "17:00", "2024-02-15T01:00:00Z", "2024-02-15T01:05:00Z"},
			{"5m", "15m", "8:00", "17:00", "2024-02-15T09:00:00Z", "2024-02-15T09:15:00Z"},
			{"5m", "15m", "8:00", "17:00", "2024-02-15T16:59:00Z", "2024-02-15T17:00:00Z"}, // crosses boundary

			// Alternate start/stop times straddle midnight.
			{"1h", "5m", "23:00", "02:00", "2024-02-15T21:00:00Z", "2024-02-15T22:00:00Z"},
			{"1h", "5m", "23:00", "02:00", "2024-02-15T22:30:00Z", "2024-02-15T23:00:00Z"},
			{"1h", "5m", "23:00", "02:00", "2024-02-15T23:30:00Z", "2024-02-15T23:35:00Z"},
			{"1h", "5m", "23:00", "02:00", "2024-02-15T21:00:00Z", "2024-02-15T22:00:00Z"},
			{"1h", "5m", "23:00", "02:00", "2024-02-15T23:57:00Z", "2024-02-16T00:00:00Z"},

			// Long delay that completely spans the time period that the short delay
			// would be active.
			{"5h", "5m", "01:00", "03:00", "2024-02-15T23:00:00Z", "2024-02-16T01:00:00Z"},

			// Inverted intervals where the delay goes into the next day.
			{"5h", "10h", "01:00", "03:00", "2024-02-15T23:00:00Z", "2024-02-16T04:00:00Z"},
			{"3h", "5h", "23:00", "01:00", "2024-02-15T23:30:00Z", "2024-02-16T01:00:00Z"},

			// Extremely long delays.
			{"72h", "5m", "08:00", "17:00", "2024-02-15T05:00:00Z", "2024-02-15T08:00:00Z"},
			{"72h", "5m", "08:00", "17:00", "2024-02-15T19:00:00Z", "2024-02-16T08:00:00Z"},
			{"72h", "5m", "08:00", "17:00", "2024-02-15T09:00:00Z", "2024-02-15T09:05:00Z"},
			{"5m", "72h", "08:00", "17:00", "2024-02-15T09:00:00Z", "2024-02-15T17:00:00Z"},
		} {
			sched, err := NewAlternatingSchedule(tc.DefaultInterval, tc.AlternateInterval, tc.AlternateStart, tc.AlternateEnd, "", "UTC", nil)
			require.NoError(t, err)
			after, err := time.Parse(time.RFC3339, tc.After)
			require.NoError(t, err)
			var ts = sched.Next(after)
			require.Equal(t, tc.Expect, ts.UTC().Format(time.RFC3339))
		}
	})

	t.Run("with active days set", func(t *testing.T) {
		for _, tc := range []struct {
			DefaultInterval     string
			AlternateInterval   string
			AlternateStart      string
			AlternateEnd        string
			AlternateActiveDays string
			After               string
			Expect              string
		}{
			// 6/28 is a Friday.
			{"4h", "5m", "08:00", "17:00", "M-F", "2024-06-28T00:00:00Z", "2024-06-28T04:00:00Z"},
			{"4h", "5m", "08:00", "17:00", "M-F", "2024-06-28T10:00:00Z", "2024-06-28T10:05:00Z"},
			{"4h", "5m", "08:00", "17:00", "M-F", "2024-06-29T00:00:00Z", "2024-06-29T04:00:00Z"},
			{"4h", "5m", "22:00", "02:00", "M-F", "2024-06-28T23:00:00Z", "2024-06-28T23:05:00Z"},
			{"7h", "5m", "22:00", "02:00", "M-F", "2024-06-30T21:00:00Z", "2024-07-01T00:00:00Z"}, // Sunday turns into Monday
			{"7h", "5m", "22:00", "02:00", "M-F", "2024-06-30T23:00:00Z", "2024-07-01T00:00:00Z"}, // also Sunday turns into Monday, but starting from the alternate interval

			{"5m", "7h", "22:00", "02:00", "M-F", "2024-06-28T23:00:00Z", "2024-06-29T00:00:00Z"},
			{"5m", "7h", "22:00", "02:00", "M-F", "2024-06-29T23:00:00Z", "2024-06-29T23:05:00Z"},

			// 6/25 is a Tuesday.
			{"72h", "5m", "08:00", "17:00", "Su,Th", "2024-06-25T00:00:00Z", "2024-06-27T08:00:00Z"}, // crosses active lower interval on Thursday
			{"72h", "5m", "08:00", "17:00", "Su", "2024-06-25T00:00:00Z", "2024-06-28T00:00:00Z"},

			// 6/21 is a Friday. In this case `after` just barely missed the
			// alternate window, and has to wrap completely around to the next
			// week's Friday until it hits another active window, since only
			// Friday is enabled.
			{"336h", "5m", "3:00", "05:00", "F", "2024-06-21T6:00:00Z", "2024-06-28T03:00:00Z"},
		} {
			sched, err := NewAlternatingSchedule(tc.DefaultInterval, tc.AlternateInterval, tc.AlternateStart, tc.AlternateEnd, tc.AlternateActiveDays, "UTC", nil)
			require.NoError(t, err)
			after, err := time.Parse(time.RFC3339, tc.After)
			require.NoError(t, err)
			var ts = sched.Next(after)
			require.Equal(t, tc.Expect, ts.UTC().Format(time.RFC3339))
		}
	})
}

func TestClockTimeBetween(t *testing.T) {
	for _, tc := range []struct {
		start string
		end   string
		loc   string
		dt    string
		want  bool
	}{
		{"03:00", "05:00", "UTC", "2024-02-15T02:00:00Z", false},
		{"03:00", "05:00", "UTC", "2024-02-15T02:59:59Z", false},
		{"03:00", "05:00", "UTC", "2024-02-15T03:00:00Z", true},
		{"03:00", "05:00", "UTC", "2024-02-15T04:00:00Z", true},
		{"03:00", "05:00", "UTC", "2024-02-15T04:59:59Z", true},
		{"03:00", "05:00", "UTC", "2024-02-15T05:00:00Z", false},
		{"03:00", "05:00", "UTC", "2024-02-15T06:00:00Z", false},
		{"22:00", "02:00", "UTC", "2024-02-15T21:00:00Z", false},
		{"22:00", "02:00", "UTC", "2024-02-15T21:59:59Z", false},
		{"22:00", "02:00", "UTC", "2024-02-15T22:00:00Z", true},
		{"22:00", "02:00", "UTC", "2024-02-15T23:00:00Z", true},
		{"22:00", "02:00", "UTC", "2024-02-15T00:00:00Z", true},
		{"22:00", "02:00", "UTC", "2024-02-15T01:00:00Z", true},
		{"22:00", "02:00", "UTC", "2024-02-15T01:59:59Z", true},
		{"22:00", "02:00", "UTC", "2024-02-15T02:00:00Z", false},
		{"22:00", "02:00", "UTC", "2024-02-15T03:00:00Z", false},

		// No daylight savings time.
		{"09:00", "17:00", "America/New_York", "2024-02-15T13:59:59Z", false},
		{"09:00", "17:00", "America/New_York", "2024-02-15T14:00:00Z", true},
		{"09:00", "17:00", "America/New_York", "2024-02-15T21:59:59Z", true},
		{"09:00", "17:00", "America/New_York", "2024-02-15T22:00:00Z", false},
		{"18:00", "20:00", "America/New_York", "2024-02-15T22:59:59Z", false},
		{"18:00", "20:00", "America/New_York", "2024-02-15T23:00:00Z", true},
		{"18:00", "20:00", "America/New_York", "2024-02-16T00:59:59Z", true},
		{"18:00", "20:00", "America/New_York", "2024-02-16T01:00:00Z", false},
		{"09:00", "17:00", "America/St_Johns", "2024-02-15T12:29:59Z", false},
		{"09:00", "17:00", "America/St_Johns", "2024-02-15T12:30:00Z", true},
		{"09:00", "17:00", "America/St_Johns", "2024-02-15T20:29:59Z", true},
		{"09:00", "17:00", "America/St_Johns", "2024-02-15T20:30:00Z", false},

		// Daylight savings time active.
		{"09:00", "17:00", "America/New_York", "2024-05-15T12:59:59Z", false},
		{"09:00", "17:00", "America/New_York", "2024-05-15T13:00:00Z", true},
		{"09:00", "17:00", "America/New_York", "2024-05-15T20:59:59Z", true},
		{"09:00", "17:00", "America/New_York", "2024-05-15T21:00:00Z", false},
		{"18:00", "20:00", "America/New_York", "2024-05-15T21:59:59Z", false},
		{"18:00", "20:00", "America/New_York", "2024-05-15T22:00:00Z", true},
		{"18:00", "20:00", "America/New_York", "2024-05-15T23:59:59Z", true},
		{"18:00", "20:00", "America/New_York", "2024-05-16T00:00:00Z", false},
		{"09:00", "17:00", "America/St_Johns", "2024-05-15T11:29:59Z", false},
		{"09:00", "17:00", "America/St_Johns", "2024-05-15T11:30:00Z", true},
		{"09:00", "17:00", "America/St_Johns", "2024-05-15T19:29:59Z", true},
		{"09:00", "17:00", "America/St_Johns", "2024-05-15T19:30:00Z", false},
	} {
		loc, err := time.LoadLocation(tc.loc)
		require.NoError(t, err)
		start, err := newClockTime(tc.start, loc)
		require.NoError(t, err)
		end, err := newClockTime(tc.end, loc)
		require.NoError(t, err)
		dt, err := time.Parse(time.RFC3339, tc.dt)
		require.NoError(t, err)
		require.Equal(t, tc.want, start.between(dt, end))
	}
}

func TestParseActiveDays(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want [7]bool
	}{
		{"", [7]bool{true, true, true, true, true, true, true}},
		{"Su-S", [7]bool{true, true, true, true, true, true, true}},
		{"M-F", [7]bool{false, true, true, true, true, true, false}},
		{"M,W,F", [7]bool{false, true, false, true, false, true, false}},
		{"Su,T-Th", [7]bool{true, false, true, true, true, false, false}},
		{"Su-M,Th-S", [7]bool{true, true, false, false, true, true, true}},
	} {
		got, err := parseActiveDays(tc.in)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}
