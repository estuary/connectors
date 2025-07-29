package testing

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

// RedirectTestLogs redirects the logrus standard logger to output messages via
// t.Logf() for the remaining duration of the current test.
func RedirectTestLogs(t testing.TB) {
	t.Helper()
	var previousOutput = log.StandardLogger().Out
	var previousFormatter = log.StandardLogger().Formatter
	var previousReportCaller = log.StandardLogger().ReportCaller
	log.SetOutput(&testLogWriter{t: t})
	log.SetFormatter(&testLogFormatter{})
	log.SetReportCaller(true)
	t.Cleanup(func() {
		log.SetOutput(previousOutput)
		log.SetFormatter(previousFormatter)
		log.SetReportCaller(previousReportCaller)
	})
}

type testLogWriter struct {
	t testing.TB
}

func (w *testLogWriter) Write(p []byte) (n int, err error) {
	if len(p) > 0 {
		w.t.Logf("%s", p)
	}
	return len(p), nil
}

type testLogFormatter struct{}

func (f *testLogFormatter) Format(entry *log.Entry) ([]byte, error) {
	var buf = new(bytes.Buffer)
	if entry.Caller != nil {
		fmt.Fprintf(buf, "%s:%d: ", filepath.Base(entry.Caller.File), entry.Caller.Line)
	}
	fmt.Fprintf(buf, "%-5s %s", strings.ToUpper(entry.Level.String()), entry.Message)
	if len(entry.Data) > 0 {
		fmt.Fprintf(buf, " (")
		for k, v := range entry.Data {
			var s, ok = v.(string)
			if !ok {
				s = fmt.Sprint(v)
			}
			fmt.Fprintf(buf, " %s=%q", k, s)
		}
		fmt.Fprintf(buf, " )")
	}
	return buf.Bytes(), nil
}
