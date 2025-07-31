package main

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/segmentio/encoding/json"
)

type mysqlCommitEvent struct {
	Position mysql.Position
}

func (mysqlCommitEvent) IsDatabaseEvent() {}
func (mysqlCommitEvent) IsCommitEvent()   {}

func (evt *mysqlCommitEvent) String() string {
	return fmt.Sprintf("Commit(%s:%d)", evt.Position.Name, evt.Position.Pos)
}

func (evt *mysqlCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	var cursorString = fmt.Sprintf("%s:%d", evt.Position.Name, evt.Position.Pos)
	return json.AppendEscape(buf, cursorString, 0), nil
}
