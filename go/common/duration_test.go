package common

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/stretchr/testify/require"
)

func TestDuration_Schema(t *testing.T) {
	type testObject struct {
		Duration Duration  `json:"duration" jsonschema:"title=Duration Property,description=This is a duration field with a title and description."`
		DurPtr   *Duration `json:"duration_ptr,omitempty" jsonschema:"title=Duration Pointer Property,description=This is a duration pointer field with a title and description."`
	}
	schema, err := schemagen.GenerateSchema("Test Object", &testObject{}).MarshalJSON()
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(json.RawMessage(schema), "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestDuration_UnmarshalObject checks unmarshalling into a struct containing Duration fields.
func TestDuration_UnmarshalObject(t *testing.T) {
	type testObject struct {
		Duration Duration  `json:"duration,omitempty"`
		DurPtr   *Duration `json:"duration_ptr,omitempty"`
		Other    int       `json:"other"`
	}
	for _, tc := range []struct {
		name     string
		input    string
		expected testObject
	}{
		{"empty", `{}`, testObject{}},
		{"only_other", `{"other": 3}`, testObject{Other: 3}},
		{"duration_zero", `{"duration": "P0D", "other": 3}`, testObject{Duration: Duration{}, Other: 3}},
		{"durptr_zero", `{"duration_ptr": "P0D", "other": 3}`, testObject{DurPtr: &Duration{}, Other: 3}},
		{"duration_1sec", `{"duration": "PT1S", "other": 3}`, testObject{Duration: Duration{TS: 1}, Other: 3}},
		{"durptr_1sec", `{"duration_ptr": "PT1S", "other": 3}`, testObject{DurPtr: &Duration{TS: 1}, Other: 3}},
		{"duration_complex", `{"duration": "PT1H30M45S", "other": 3}`, testObject{Duration: Duration{TH: 1, TM: 30, TS: 45}, Other: 3}},
		{"durptr_complex", `{"duration_ptr": "PT1H30M45S", "other": 3}`, testObject{DurPtr: &Duration{TH: 1, TM: 30, TS: 45}, Other: 3}},
		{"duration_empty", `{"duration": "", "other": 3}`, testObject{Duration: Duration{}, Other: 3}},  // Note: empty string is treated as zero duration.
		{"durptr_empty", `{"duration_ptr": "", "other": 3}`, testObject{DurPtr: &Duration{}, Other: 3}}, // Note: empty string is treated as zero duration.
		{"duration_null", `{"duration": null, "other": 3}`, testObject{Duration: Duration{}, Other: 3}}, // Note: null is treated as zero duration.
		{"durptr_null", `{"duration_ptr": null, "other": 3}`, testObject{DurPtr: nil, Other: 3}},        // Note: null is treated as nil pointer.
	} {
		t.Run(tc.name, func(t *testing.T) {
			var parsed testObject
			require.NoError(t, json.Unmarshal([]byte(tc.input), &parsed))
			require.Equal(t, tc.expected, parsed)
		})
	}
}

func TestDuration_NilAsDuration(t *testing.T) {
	var d *Duration = nil
	require.Equal(t, d.AsDuration(), time.Duration(0), "Nil Duration should be treated as zero duration")
}

func TestDuration_MarshalJSON(t *testing.T) {
	for _, tc := range []struct {
		name     string
		duration Duration
		expected string
	}{
		{"zero", Duration{}, `"P0D"`},
		{"1sec", Duration{TS: 1}, `"PT1S"`},
		{"1min", Duration{TM: 1}, `"PT1M"`},
		{"1hr", Duration{TH: 1}, `"PT1H"`},
		{"1day", Duration{D: 1}, `"P1D"`},
		{"negative", Duration{TS: -1}, `"P-1S"`}, // Note: this is a bug in the underlying implementation, it should be `"PT-1S"`
		{"complex", Duration{TH: 1, TM: 30, TS: 45}, `"PT1H30M45S"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.duration.MarshalJSON()
			if err != nil {
				t.Fatalf("MarshalJSON failed: %v", err)
			}
			if string(data) != tc.expected {
				t.Errorf("MarshalJSON() = %s, want %s", string(data), tc.expected)
			}
		})
	}
}

func TestDuration_UnmarshalJSON(t *testing.T) {
	for _, tc := range []struct {
		name      string
		json      string
		expected  time.Duration
		expectErr bool
	}{
		{"1sec", `"PT1S"`, time.Second, false},
		{"1min", `"PT1M"`, time.Minute, false},
		{"1hr", `"PT1H"`, time.Hour, false},
		{"1day", `"P1D"`, 24 * time.Hour, false},
		{"0day", `"P0D"`, 0, false},
		{"0sec", `"PT0S"`, 0, false},
		{"empty", `""`, 0, false},    // Note: This is not a valid ISO 8601 duration, but we have logic to treat empty strings as zero durations.
		{"justp", `"P"`, 0, false},   // Note: Strictly speaking this is not valid ISO 8601, at least according to RFC 3339 Appendix A, but it is accepted by the underlying library.
		{"justpt", `"PT"`, 0, false}, // Note: Strictly speaking this is not valid ISO 8601, at least according to RFC 3339 Appendix A, but it is accepted by the underlying library.
		{"complex", `"PT1H30M45S"`, time.Hour + 30*time.Minute + 45*time.Second, false},
		{"invalid format", `"invalid"`, 0, true},
		{"invalid json", `invalid`, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d Duration
			err := d.UnmarshalJSON([]byte(tc.json))
			if (err != nil) != tc.expectErr {
				t.Errorf("UnmarshalJSON(%q) error = %v, expectErr %v", tc.json, err, tc.expectErr)
				return
			}
			if !tc.expectErr && d.AsDuration() != tc.expected {
				t.Errorf("UnmarshalJSON(%q) = %v, want %v", tc.json, d, tc.expected)
			}
		})
	}
}

func TestDuration_JSONRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name     string
		duration Duration
	}{
		{"zero", Duration{}},
		{"1sec", Duration{TS: 1}},
		{"complex", Duration{TH: 1, TM: 30, TS: 45}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.duration)
			require.NoError(t, err)
			var restored Duration
			require.NoError(t, json.Unmarshal(data, &restored))
			if tc.duration != restored {
				t.Errorf("Round trip failed: original %v, restored %v", tc.duration, restored)
			}
		})
	}
}
