package sqlcapture

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

var timeZoneOffsetRegex = regexp.MustCompile(`^[-+][0-9]{1,2}:[0-9]{2}$`)

var errInvalidTimezone = errors.New("timezone must be a valid IANA timezone name or +HH:MM offset")

// ParseTimezone parses a timezone name or numeric offset into a time.Location.
func ParseTimezone(tzName string) (*time.Location, error) {
	// If the time zone setting is a valid IANA zone name then return that.
	loc, err := time.LoadLocation(tzName)
	if err == nil {
		return loc, nil
	}

	// If it looks like a numeric offset then parse a fixed-offset time zone from that.
	if timeZoneOffsetRegex.MatchString(tzName) {
		var t, err = time.Parse("-07:00", tzName)
		if err != nil {
			return nil, fmt.Errorf("error parsing %q: %w", tzName, err)
		}
		return t.Location(), nil
	}

	return nil, fmt.Errorf("unknown or invalid timezone %q: %w", tzName, errInvalidTimezone)
}
