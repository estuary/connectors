package sql

import (
	"encoding/base64"
	"strings"
	"text/template"

	log "github.com/sirupsen/logrus"
)

type ColumnWithAlias struct {
	Column
	Alias string
}

func (c ColumnWithAlias) AsFlatType() FlatType {
	t, _ := c.Column.AsFlatType()
	return t
}

func (c ColumnWithAlias) Format() string {
	return c.Inference.String_.Format
}

// MustParseTemplate is a convenience which parses the template `body` and
// installs common functions for accessing Dialect behavior.
func MustParseTemplate(dialect Dialect, name, body string, extraFuncs ...template.FuncMap) *template.Template {
	funcs := template.FuncMap{
		"Literal":          dialect.Literal,
		"Base64Std":        base64.StdEncoding.EncodeToString,
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
		"Backtick":   func() string { return "`" }, // Go string literals don't allow a ` character
		"ColumnWithAlias": func(c Column, alias string) ColumnWithAlias {
			return ColumnWithAlias{Column: c, Alias: alias}
		},
		"ChunkColumns": func(cols []*Column, size int) [][]*Column {
			if size <= 0 {
				return [][]*Column{cols}
			}
			var chunks [][]*Column
			for i := 0; i < len(cols); i += size {
				end := i + size
				if end > len(cols) {
					end = len(cols)
				}
				chunks = append(chunks, cols[i:end])
			}
			return chunks
		},
	}
	funcs["BoundOn"] = func(alias string, bound MergeBound) BoundOn { return BoundOn{Alias: alias, Bound: bound} }
	for _, extra := range extraFuncs {
		for k, v := range extra {
			funcs[k] = v
		}
	}
	return template.Must(template.Must(template.New(name).Funcs(funcs).Parse(boundConditionTemplate)).Parse(body))
}

// boundConditionTemplate defines "boundCondition", which renders the range
// condition of a BoundOn as one range of its lower and upper literals, or as
// the union of its ranges when it has them.
const boundConditionTemplate = `
{{ define "boundCondition" -}}
{{- if $.Bound.Ranges }} AND (
	{{- range $i, $r := $.Bound.Ranges }}{{ if $i }} OR {{ end -}}
	({{ $.Alias }}.{{ $.Bound.Identifier }} >= {{ $r.LiteralLower }} AND {{ $.Alias }}.{{ $.Bound.Identifier }} <= {{ $r.LiteralUpper }})
	{{- end }})
{{- else if $.Bound.LiteralLower }} AND {{ $.Alias }}.{{ $.Bound.Identifier }} >= {{ $.Bound.LiteralLower }} AND {{ $.Alias }}.{{ $.Bound.Identifier }} <= {{ $.Bound.LiteralUpper }}
{{- end }}
{{- end }}
`

// BoundOn is a merge bound on a key column of the table under Alias.
type BoundOn struct {
	Alias string
	Bound MergeBound
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
