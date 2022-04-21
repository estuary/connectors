package main

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
)

type flowDocument struct {
	Body json.RawMessage
}

func (fd *flowDocument) Load(v []bigquery.Value, s bigquery.Schema) error {
	if document, ok := v[0].(string); !ok {
		return fmt.Errorf("value[0] wrong type %T expecting string", v[0])
	} else {
		fd.Body = json.RawMessage(document)
	}
	return nil

}
