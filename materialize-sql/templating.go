package sql

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/pierrec/lz4/v4"
	log "github.com/sirupsen/logrus"
)

// MustParseTemplate is a convenience which parses the template `body` and
// installs common functions for accessing Dialect behavior.
func MustParseTemplate(dialect Dialect, name, body string) *template.Template {
	var tpl = template.New(name).Funcs(template.FuncMap{
		"Literal":   dialect.Literal,
		"Base64Std": base64.StdEncoding.EncodeToString,
		"LZ4Compress": func(src []byte) ([]byte, error) {
			var out bytes.Buffer
			bufWriter := bufio.NewWriter(&out)
			var writer = lz4.NewWriter(bufWriter)

			if _, err := io.Copy(writer, bytes.NewReader(src)); err != nil {
				return nil, err
			}
			// Need to both close/flush the lz4 writer, and the bufio.Writer
			if err := writer.Close(); err != nil {
				return nil, err
			}
			if err := bufWriter.Flush(); err != nil {
				return nil, err
			}

			fmt.Printf("In: %v, Out: %v", src, out)

			return out.Bytes(), nil
		},
		"ColumnIdentifier": dialect.Identifier,
		// Tweak signature slightly to take TablePath, as dynamic slicing is a bit tricky
		// in templates and this is most-frequently used with TablePath.Base().
		"Identifier": func(p TablePath) string { return dialect.Identifier(p...) },
		"Join":       func(s []string, delim string) string { return strings.Join(s, delim) },
		"Split":      func(s string, delim string) []string { return strings.Split(s, delim) },
		"Repeat":     func(n int) []bool { return make([]bool, n) },
		"Add":        func(a, b int) int { return a + b },
		"Contains":   func(s string, substr string) bool { return strings.Contains(s, substr) },
		"Last":       func(s []string) string { return s[len(s)-1] },
		"First":      func(s []string) string { return s[0] },
	})
	return template.Must(tpl.Parse(body))
}

// RenderTableTemplate is a simple implementation of rendering a template with a Table
// as its context. It's here for demonstration purposes mostly. Feel free to not use it.
func RenderTableTemplate(table Table, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &table); err != nil {
		return "", err
	}
	var s = w.String()
	log.WithField("rendered", s).WithField("table", table).Debug("rendered template")
	return s, nil
}
