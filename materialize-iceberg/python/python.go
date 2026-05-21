package python

import log "github.com/sirupsen/logrus"

// ForwardLogs emits each entry from a Python job's status log list through the
// Go connector's logger. Each entry must have a "msg" key; all other keys
// become log fields. Entries without "msg" are silently skipped.
func ForwardLogs(entries []map[string]interface{}) {
	for _, entry := range entries {
		msg, ok := entry["msg"].(string)
		if !ok {
			continue
		}
		fields := make(log.Fields, len(entry)-1)
		for k, v := range entry {
			if k != "msg" {
				fields[k] = v
			}
		}
		log.WithFields(fields).Info(msg)
	}
}

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
	Binding int           `json:"binding"`
	Query   string        `json:"query"`
	Columns []NestedField `json:"columns"`
	Files   []string      `json:"files"`
}

type MergeInput struct {
	Bindings []MergeBinding `json:"bindings"`
}

type StatusOutput struct {
	Success bool                     `json:"success"`
	Error   string                   `json:"error"`
	Logs    []map[string]interface{} `json:"logs,omitempty"`
}
