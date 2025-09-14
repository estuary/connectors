package main

import (
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
	sf "github.com/snowflakedb/gosnowflake"
)

var featureFlagDefaults = map[string]bool{
	// Use Snowpipe streaming for delta-updates bindings that use JWT
	// authentication.
	"snowpipe_streaming": true,
}

type config struct {
	Host          string                           `json:"host" jsonschema:"title=Host (Account URL),description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0,pattern=^[^/:]+.snowflakecomputing.com$"`
	Database      string                           `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=3"`
	Schema        string                           `json:"schema" jsonschema:"title=Schema,description=Database schema for bound collection tables (unless overridden within the binding resource configuration)." jsonschema_extras:"order=4"`
	Warehouse     string                           `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries. Uses the default warehouse for the Snowflake user if left blank." jsonschema_extras:"order=5"`
	Role          string                           `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=6"`
	Account       string                           `json:"account,omitempty" jsonschema:"title=Account,description=Optional Snowflake account identifier." jsonschema_extras:"order=7,x-hidden-field=true"`
	HardDelete    bool                             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=8"`
	Credentials   *snowflake_auth.CredentialConfig `json:"credentials" jsonschema:"title=Authentication"`
	Schedule      m.ScheduleConfig                 `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger dbt.JobConfig                    `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`
	Advanced      advancedConfig                   `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	DisableFieldTruncation bool   `json:"disableFieldTruncation,omitempty" jsonschema:"title=Disable Field Truncation,description=Disables truncation of materialized fields. May result in errors for documents with extremely large values or complex nested structures."`
	NoFlowDocument         bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	FeatureFlags           string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

// toURI manually builds the DSN connection string. Most uses should set
// `includeSchema`, to preserve legacy behavior where having the schema set on
// the connection level is necessary for queries involving tables that don't
// have the schema as part of their resource path.
func (c config) toURI(includeSchema bool) (string, error) {
	var uri = url.URL{
		Host: c.Host + ":443",
	}

	queryParams := make(url.Values)

	if includeSchema {
		queryParams.Add("schema", c.Schema)
	}

	// Required params
	queryParams.Add("application", "EstuaryFlow")
	queryParams.Add("database", c.Database)
	// GO_QUERY_RESULT_FORMAT is json in order to enable stream downloading of load results.
	queryParams.Add("GO_QUERY_RESULT_FORMAT", "json")
	// By default Snowflake expects the number of statements to be provided
	// with every request. By setting this parameter to zero we are allowing a
	// variable number of statements to be executed in a single request.
	queryParams.Add("MULTI_STATEMENT_COUNT", "0")
	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	queryParams.Add("client_session_keep_alive", "true")
	queryParams.Add("maxRetryCount", "10")

	// Optional params
	if c.Warehouse != "" {
		queryParams.Add("warehouse", c.Warehouse)
	}

	if c.Role != "" {
		queryParams.Add("role", c.Role)
	}

	if c.Account != "" {
		queryParams.Add("account", c.Account)
	}

	// Authentication
	var user string
	if c.Credentials.AuthType == snowflake_auth.UserPass {
		user = url.QueryEscape(c.Credentials.User) + ":" + url.QueryEscape(c.Credentials.Password)
	} else if c.Credentials.AuthType == snowflake_auth.JWT {
		// We run this as part of validate to ensure that there is no error, so
		// this is not expected to error here.
		if key, err := c.Credentials.ParsePrivateKey(); err != nil {
			return "", err
		} else if privateKey, err := x509.MarshalPKCS8PrivateKey(key); err != nil {
			return "", fmt.Errorf("parsing private key: %w", err)
		} else {
			privateKeyString := base64.URLEncoding.EncodeToString(privateKey)
			queryParams.Add("privateKey", privateKeyString)
			queryParams.Add("authenticator", strings.ToLower(sf.AuthTypeJwt.String()))
		}
		user = url.QueryEscape(c.Credentials.User)
	} else {
		return "", fmt.Errorf("unknown auth type: %s", c.Credentials.AuthType)
	}

	dsn := user + "@" + uri.Hostname() + ":" + uri.Port() + "?" + queryParams.Encode()
	return dsn, nil
}

var hostRe = regexp.MustCompile(`(?i)^.+.snowflakecomputing\.com$`)

func validHost(h string) error {
	hasProtocol := strings.Contains(h, "://")
	missingDomain := !hostRe.MatchString(h)

	if hasProtocol && missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com and not include a protocol)", h)
	} else if hasProtocol {
		return fmt.Errorf("invalid host %q (must not include a protocol)", h)
	} else if missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", h)
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return c.Schema
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"database", c.Database},
		{"schema", c.Schema},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	if err := c.Credentials.Validate(); err != nil {
		return err
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return validHost(c.Host)
}
