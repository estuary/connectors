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
	// Table identifies the table to the job, for reading back the snapshot
	// its merge committed. It's set when the job is run, not checkpointed.
	Table string `json:"table,omitempty"`
}

type MergeInput struct {
	Bindings []MergeBinding `json:"bindings"`
}

type StatusOutput struct {
	Success bool     `json:"success"`
	Error   string   `json:"error"`
	Stats   JobStats `json:"stats"`
}

// JobStats is what a load or merge job measured of its own work.
type JobStats struct {
	// QueryMs is how long a load's query ran, once Spark had started.
	QueryMs  int64          `json:"query_ms"`
	Bindings []BindingStats `json:"bindings"`
}

// BindingStats is what a merge job measured of one binding.
type BindingStats struct {
	Binding    int   `json:"binding"`
	MergeMs    int64 `json:"merge_ms"`
	TruncateMs int64 `json:"truncate_ms"`
	// Retries counts the merges that lost to a concurrent commit.
	Retries int `json:"retries"`
	// Commit is the operation and summary of the snapshot the merge
	// committed.
	Commit map[string]string `json:"commit"`
}
