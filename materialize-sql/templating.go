package sql

import (
	"encoding/base64"
	"strings"
	"text/template"
)

// MustParseTemplate is a convenience which parses the template `body` and
// installs common functions for accessing Dialect behavior.
func MustParseTemplate(dialect Dialect, name, body string) *template.Template {
	var tpl = template.New(name).Funcs(template.FuncMap{
		"Literal":   dialect.Literal,
		"Base64Std": base64.StdEncoding.EncodeToString,
		// Tweak signature slightly to take TablePath, as dynamic slicing is a bit tricky
		// in templates and this is most-frequently used with TablePath.Base().
		"Identifier": func(p TablePath) string { return dialect.Identifier(p...) },
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
	return w.String(), nil
}
