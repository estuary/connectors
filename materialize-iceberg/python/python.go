package python

type NestedField struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Element string `json:"element,omitempty"`
	// VariantFormat hints how top-level string values of a "variant" column
	// are typed: "date-time", "date", or "binary". Absent for other types and
	// for variants that store strings as strings.
	VariantFormat string `json:"variant_format,omitempty"`
}

type ExecInput struct {
	Query string `json:"query"`
}

type LoadBinding struct {
	Binding int           `json:"binding"`
	Keys    []NestedField `json:"keys"`
	Files   []string      `json:"files"`
}

type LoadInput struct {
	Query          string        `json:"query"`
	Bindings       []LoadBinding `json:"bindings"`
	OutputLocation string        `json:"output_location"`
}

type MergeBinding struct {
	Binding          int           `json:"binding"`
	Query            string        `json:"query"`
	Columns          []NestedField `json:"columns"`
	Files            []string      `json:"files"`
	IdempotencyToken string        `json:"idempotency_token"`
}

type MergeInput struct {
	Bindings []MergeBinding `json:"bindings"`
}

type StatusOutput struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
