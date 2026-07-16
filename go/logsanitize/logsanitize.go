// Package logsanitize marks customer-derived values that are intentionally
// included in a log line or error message.
//
// Some errors genuinely need to carry a data value to be actionable — e.g. the
// value that failed a type conversion. Rather than drop the value (losing
// debuggability) or leak it unconditionally, wrap it with one of the helpers
// here. The value is bracketed with a pair of sentinel code points that ride
// along through fmt.Errorf/%w wrapping and logrus JSON encoding untouched, so a
// downstream log-obfuscation pass (see obfuscate_logs.py) can locate the span
// and redact or obfuscate the value.
//
// The single import path is also the inventory: `grep -rn logsanitize.` lists
// every site that knowingly emits customer data. Do not emit customer values
// into logs or errors by any other means.
package logsanitize

import (
	"fmt"
	"strconv"
)

// Open and Close bracket a marked value. They are Unicode Private-Use-Area code
// points: they never occur in real text data, are valid UTF-8, survive JSON
// encoding as literal bytes (Go's encoder does not escape them), and are
// trivially greppable. The obfuscation script matches U+E000 ... U+E001.
const (
	Open  = ""
	Close = ""
)

// Value marks the default (%v) rendering of v as customer data.
func Value(v any) string {
	return Open + fmt.Sprintf("%v", v) + Close
}

// Quoted marks a Go-quoted rendering of v, used where the original site logged
// the value with %q. The quotes are inside the marked span. Do not additionally
// wrap the result in %q.
func Quoted(v any) string {
	return Open + strconv.Quote(fmt.Sprint(v)) + Close
}

// Goval marks a Go-syntax (%#v) rendering of v, used where the original site
// logged the value with %#v (e.g. a value of otherwise-unknown type).
func Goval(v any) string {
	return Open + fmt.Sprintf("%#v", v) + Close
}
