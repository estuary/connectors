package main

import (
	"encoding/base64"
	"fmt"
)

type sqlserverCommitEvent struct {
	CommitLSN LSN
}

func (sqlserverCommitEvent) IsDatabaseEvent() {}
func (sqlserverCommitEvent) IsCommitEvent()   {}

func (evt *sqlserverCommitEvent) String() string {
	return fmt.Sprintf("Commit(%X)", evt.CommitLSN)
}

func (evt *sqlserverCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	// Since base64 strings never require escaping we can just splat it between quotes.
	buf = append(buf, '"')
	buf = base64.StdEncoding.AppendEncode(buf, evt.CommitLSN)
	return append(buf, '"'), nil
}
