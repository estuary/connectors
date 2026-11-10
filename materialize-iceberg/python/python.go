package python

type NestedField struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Element string `json:"element,omitempty"`
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
	// TruncateQuery deletes rows published before a completed backfill's
	// boundary. Empty if the binding has no backfill truncation pending.
	TruncateQuery string `json:"truncate_query,omitempty"`
}

type MergeInput struct {
	Bindings []MergeBinding `json:"bindings"`
}

type StatusOutput struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
