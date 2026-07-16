package logsanitize

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// span matches a marked value the way the log-obfuscation script does.
var span = regexp.MustCompile("(.*?)")

func TestMarkersSurviveWrappingAndJSON(t *testing.T) {
	// A value marked deep inside a wrapped error must still be locatable after
	// %w wrapping and after the whole message is JSON-encoded (as logrus does).
	inner := fmt.Errorf("could not parse %s as RFC3339 date-time", Quoted("2020-01-02T03:04:05Z"))
	wrapped := fmt.Errorf("converting tuple index %d: %w", 3, inner)

	msg := wrapped.Error()
	if got := span.FindStringSubmatch(msg); got == nil {
		t.Fatalf("marked span not found in wrapped error: %q", msg)
	} else if want := `"2020-01-02T03:04:05Z"`; got[1] != want {
		t.Fatalf("captured %q, want %q", got[1], want)
	}

	// Round-trip through JSON: the PUA markers must decode back to the same
	// code points so the script can match them in JSON-formatted logs.
	encoded, err := json.Marshal(map[string]string{"message": msg})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]string
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !span.MatchString(decoded["message"]) {
		t.Fatalf("marked span lost through JSON round-trip: %q", decoded["message"])
	}
}

func TestHelpersBracketExactlyOnce(t *testing.T) {
	for _, s := range []string{Value(42), Quoted("x"), Goval([]int{1})} {
		if strings.Count(s, Open) != 1 || strings.Count(s, Close) != 1 {
			t.Errorf("expected exactly one open/close pair, got %q", s)
		}
		if !strings.HasPrefix(s, Open) || !strings.HasSuffix(s, Close) {
			t.Errorf("markers not at the boundaries: %q", s)
		}
	}
}
