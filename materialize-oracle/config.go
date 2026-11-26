package connector

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/estuary/connectors/go/common"
)

var featureFlagDefaults = map[string]common.FlagDefault{
	"retain_existing_data_on_backfill": common.FlagDisabled,
	"truncate_after_backfill":          common.FlagEnabled,
}

type advancedConfig struct {
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false" jsonschema_extras:"nonsensitive=true"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

// config represents the endpoint configuration for Oracle.
type config struct {
	Address     string `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 1521 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	ServiceName string `json:"service_name" jsonschema:"title=Service Name,description=Oracle service name to connect to (for example FREEPDB1)." jsonschema_extras:"order=1"`
	User        string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=2"`
	Password    string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=3"`
	Schema      string `json:"schema,omitempty" jsonschema:"title=Database Schema,description=Schema for bound tables and the FLOW_CHECKPOINTS_V1 metadata table. Defaults to the database user's schema." jsonschema_extras:"order=4"`
	HardDelete  bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If enabled items deleted in the source will also be deleted from the destination. By default _meta/op in the destination signifies whether rows have been deleted.,default=false" jsonschema_extras:"order=5,nonsensitive=true"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

func (c config) Validate() error {
	for _, req := range [][]string{
		{"address", c.Address},
		{"service_name", c.ServiceName},
		{"user", c.User},
		{"password", c.Password},
	} {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if _, err := normalizeAddress(c.Address); err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}
	if c.Schema != "" {
		return validateIdentifier(c.Schema)
	}
	return nil
}

func (c config) DefaultNamespace() string {
	if c.Schema != "" {
		return c.Schema
	}
	return c.User
}

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

const defaultPort = "1521"

func normalizeAddress(address string) (string, error) {
	if address == "" || strings.ContainsAny(address, "/@?# \t\r\n") {
		return "", fmt.Errorf("expected host[:port]")
	}
	if !strings.Contains(address, ":") {
		address = net.JoinHostPort(address, defaultPort)
	} else if net.ParseIP(strings.Trim(address, "[]")) != nil {
		address = net.JoinHostPort(strings.Trim(address, "[]"), defaultPort)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return "", fmt.Errorf("expected host[:port]")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("port must be an integer between 1 and 65535")
	}
	return address, nil
}

// ToURI converts the config to a go-ora connection string.
func (c config) ToURI() (*url.URL, error) {
	address, err := normalizeAddress(c.Address)
	if err != nil {
		return nil, err
	}
	return &url.URL{
		Scheme:   "oracle",
		Host:     address,
		User:     url.UserPassword(c.User, c.Password),
		Path:     "/" + c.ServiceName,
		RawQuery: url.Values{"CONNECTION TIMEOUT": {"20"}}.Encode(),
	}, nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)" jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("missing table")
	}
	if err := validateIdentifier(c.Table); err != nil {
		return err
	}
	if c.Schema != "" {
		return validateIdentifier(c.Schema)
	}
	return nil
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	if c.Schema == "" {
		c.Schema = cfg.DefaultNamespace()
	}
	return c
}

func (c tableConfig) Parameters() ([]string, bool, error) {
	if c.Schema == "" {
		return []string{c.Table}, c.Delta, nil
	}
	return []string{c.Schema, c.Table}, c.Delta, nil
}

func validateIdentifier(name string) error {
	if invalidIdentifier(name) != "" {
		return fmt.Errorf("identifier must be at most %d bytes and cannot contain a double quote or NUL", maxIdentifierBytes)
	}
	return nil
}
