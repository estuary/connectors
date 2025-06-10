package common

import (
	"encoding/json"
	"time"

	"github.com/invopop/jsonschema"
	iso8601 "github.com/senseyeio/duration"
)

// Duration represents an ISO 8601 duration value.
type Duration iso8601.Duration

func (Duration) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:   "string",
		Format: "duration",
	}
}

func (x Duration) String() string               { return iso8601.Duration(x).String() }
func (x Duration) MarshalJSON() ([]byte, error) { return iso8601.Duration(x).MarshalJSON() }

func (x *Duration) Parse(s string) error {
	// Explicitly making the empty string a valid (zero) duration for maximum compatibility.
	if s == "" {
		*x = Duration{}
		return nil
	}

	p, err := iso8601.ParseISO8601(s)
	if err != nil {
		return err
	}
	*x = Duration(p)
	return nil
}

func (x *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return x.Parse(s)
}

// AsDuration converts the ISO 8601 duration to a Go time.Duration. It approximates
// days/weeks/months/years as the obvious constants, which is not strictly accurate
// but is the most useful behavior when we need a Go time.Duration value.
func (x *Duration) AsDuration() time.Duration {
	if x == nil {
		return 0
	}
	return time.Duration(x.TH)*time.Hour +
		time.Duration(x.TM)*time.Minute +
		time.Duration(x.TS)*time.Second +
		time.Duration(x.D)*24*time.Hour +
		time.Duration(x.W)*7*24*time.Hour +
		time.Duration(x.M)*30*24*time.Hour +
		time.Duration(x.Y)*365*24*time.Hour
}
