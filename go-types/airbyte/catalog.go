package airbyte

import (
	"encoding/json"
	"fmt"

	"github.com/estuary/connectors/go-types/shardrange"
)

type SyncMode string

const (
	SyncModeIncremental SyncMode = "incremental"
	SyncModeFullRefresh SyncMode = "full_refresh"
)

var AllSyncModes = []SyncMode{SyncModeIncremental, SyncModeFullRefresh}

type Stream struct {
	Name                    string          `json:"name"`
	JSONSchema              json.RawMessage `json:"json_schema"`
	SupportedSyncModes      []SyncMode      `json:"supported_sync_modes"`
	SourceDefinedCursor     bool            `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string        `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey []string        `json:"source_defined_primary_key,omitempty"`
	Namespace               string          `json:"namespace,omitempty"`
}

func (s *Stream) Validate() error {
	if len(s.SupportedSyncModes) == 0 {
		return fmt.Errorf("stream must have at least one supported_sync_modes")
	}
	return nil
}

type DestinationSyncMode string

const (
	DestinationSyncModeAppend      DestinationSyncMode = "append"
	DestinationSyncModeOverwrite   DestinationSyncMode = "overwrite"
	DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"
)

var AllDestinationSyncModes = []DestinationSyncMode{
	DestinationSyncModeAppend,
	DestinationSyncModeOverwrite,
	DestinationSyncModeAppendDedup,
}

type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	CursorField         []string            `json:"cursor_field,omitempty"`
	PrimaryKey          []string            `json:"primary_key,omitempty"`
}

func (c *ConfiguredStream) Validate() error {
	var err = c.Stream.Validate()
	if err != nil {
		return fmt.Errorf("stream invalid: %w", err)
	}
	var syncTypeValid = false
	for _, m := range c.Stream.SupportedSyncModes {
		if m == c.SyncMode {
			syncTypeValid = true
		}
	}
	if !syncTypeValid {
		return fmt.Errorf("unsupported syncMode: %s", c.SyncMode)
	}
	return nil
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
	Tail    bool               `json:"tail"`
	Range   shardrange.Range   `json:"range"`
}

func (c *ConfiguredCatalog) Validate() error {
	if len(c.Streams) == 0 {
		return fmt.Errorf("catalog must have at least one stream")
	}
	for i, s := range c.Streams {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("Streams[%d]: %w", i, err)
		}
	}
	if err := c.Range.Validate(); err != nil {
		return fmt.Errorf("Range: %w", err)
	}
	return nil
}

type Status string

const (
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

type ConnectionStatus struct {
	Status  Status `json:"status"`
	Message string `json:"message"`
}

type Record struct {
	Stream    string          `json:"stream"`
	Data      json.RawMessage `json:"data"`
	EmittedAt int64           `json:"emitted_at"`
	Namespace string          `json:"namespace,omitempty"`
}

type LogLevel string

const (
	LogLevelTrace LogLevel = "TRACE"
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
)

type Log struct {
	Level   LogLevel `json:"level"`
	Message string   `json:"message"`
}

type State struct {
	// Data is the actual state associated with the ingestion. This must be a JSON _Object_ in order
	// to comply with the airbyte specification.
	Data json.RawMessage `json:"data"`
}

type Spec struct {
	DocumentationURL              string                `json:"documentationUrl,omitempty"`
	ChangelogURL                  string                `json:"changelogUrl,omitempty"`
	ConnectionSpecification       json.RawMessage       `json:"connectionSpecification"`
	SupportsIncremental           bool                  `json:"supportsIncremental,omitempty"`
	SupportedDestinationSyncModes []DestinationSyncMode `json:"supported_destination_sync_modes,omitempty"`
}

type MessageType string

const (
	MessageTypeRecord           MessageType = "RECORD"
	MessageTypeState            MessageType = "STATE"
	MessageTypeLog              MessageType = "LOG"
	MessageTypeSpec             MessageType = "SPEC"
	MessageTypeConnectionStatus MessageType = "CONNECTION_STATUS"
	MessageTypeCatalog          MessageType = "CATALOG"
)

type Message struct {
	Type             MessageType       `json:"type"`
	Log              *Log              `json:"log,omitempty"`
	State            *State            `json:"state,omitempty"`
	Record           *Record           `json:"record,omitempty"`
	ConnectionStatus *ConnectionStatus `json:"connectionStatus,omitempty"`
	Spec             *Spec             `json:"spec,omitempty"`
	Catalog          *Catalog          `json:"catalog,omitempty"`
}

func NewLogMessage(level LogLevel, msg string, args ...interface{}) Message {
	return Message{
		Type: MessageTypeLog,
		Log: &Log{
			Level:   level,
			Message: fmt.Sprintf(msg, args...),
		},
	}
}
