package boilerplate

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/estuary/connectors/go/protocol"
	log "github.com/sirupsen/logrus"
)

func exitWithError(err error) {
	log.Error(err)
	os.Exit(1)
}

func mustEmit(responseKey protocol.ResponseCommand, payload interface{}) {
	out, err := json.Marshal(map[protocol.ResponseCommand]interface{}{
		responseKey: payload,
	})
	if err != nil {
		panic(fmt.Errorf("marshalling response: %w", err))
	}

	if _, err := fmt.Fprintln(outputWriter, string(out)); err != nil {
		panic(fmt.Errorf("writing emit output: %w", err))
	}
}

func mustParseCommand(command []byte) interface{} {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(command, &parsed); err != nil {
		panic(err)
	}

	// The JSON body of every command must have a single key, where the key is the name of the
	// command. The value is the command itself.
	if len(parsed) != 1 {
		panic(fmt.Sprintf("received command had %d keys when it must have 1", len(parsed)))
	}

	var cmd string
	var msg json.RawMessage
	for k, v := range parsed {
		cmd = k
		msg = v
	}

	switch protocol.RequestCommand(cmd) {
	case protocol.Spec:
		var req protocol.SpecRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling SpecRequest: %w", err))
		}
		return req
	case protocol.Validate:
		var req protocol.ValidateRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling ValidateRequest: %w", err))
		}
		return req
	case protocol.Apply:
		var req protocol.ApplyRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling ApplyRequest: %w", err))
		}
		return req
	case protocol.Open:
		var req protocol.OpenRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling OpenRequest: %w", err))
		}
		return req
	case protocol.Acknowledge:
		var req protocol.AcknowledgeRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling AcknowledgeRequest: %w", err))
		}
		return req
	case protocol.Load:
		var req protocol.LoadRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling LoadRequest: %w", err))
		}
		return req
	case protocol.Flush:
		var req protocol.FlushRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling FlushRequest: %w", err))
		}
		return req
	case protocol.Store:
		var req protocol.StoreRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling StoreRequest: %w", err))
		}
		return req
	case protocol.StartCommit:
		var req protocol.StartCommitRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			panic(fmt.Errorf("unmarshalling StartCommitRequest: %w", err))
		}
		return req
	default:
		panic(fmt.Sprintf("invalid command: %s", cmd))
	}
}
