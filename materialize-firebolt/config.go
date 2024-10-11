package main

import (
	"fmt"
	"strings"
)

type config struct {
	ClientId     string `json:"client_id" jsonschema:"title=Client ID" jsonschema_extras:"order=0"`
	ClientSecret string `json:"client_secret" jsonschema:"title=Client Secret" jsonschema_extras:"secret=true,order=1"`
	AccountName  string `json:"account_name" jsonschema:"title=Account Name" jsonschema_extras:"order=2"`
	EngineName   string `json:"engine_name" jsonschema:"title=Engine Name" jsonschema_extras:"order=3"`
	Database     string `json:"database" jsonschema:"title=Database" jsonschema_extras:"order=4"`
	S3Bucket     string `json:"s3_bucket" jsonschema:"title=S3 Bucket" jsonschema_extras:"order=5"`
	S3Prefix     string `json:"s3_prefix,omitempty" jsonschema:"title=S3 Prefix,default=/" jsonschema_extras:"order=6"`
	AWSKeyId     string `json:"aws_key_id,omitempty" jsonschema:"title=AWS Key ID" jsonschema_extras:"order=7"`
	AWSSecretKey string `json:"aws_secret_key,omitempty" jsonschema:"title=AWS Secret Key" jsonschema_extras:"secret=true,order=8"`
	AWSRegion    string `json:"aws_region,omitempty" jsonschema:"title=AWS Region" jsonschema_extras:"order=9"`
}

func (c config) Validate() error {
	if c.EngineName == "" {
		return fmt.Errorf("missing required engine_name")
	}
	if c.Database == "" {
		return fmt.Errorf("missing required database")
	}
	if c.AccountName == "" {
		return fmt.Errorf("missing required account_name")
	}
	if c.ClientId == "" {
		return fmt.Errorf("missing required client_id")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("missing required client_secret")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("missing required bucket")
	}
	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface
// which provides the jsonschema description of the fields
func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "EngineName":
		return "Engine Name to process your queries."
	case "Database":
		return "Name of the Firebolt database."
	case "ClientID":
		return "ID of your Firebolt service account."
	case "ClientSecret":
		return "Secret key of your Firebolt service account."
	case "AccountName":
		return "Firebolt account within your organization."
	case "S3Bucket":
		return "Name of S3 bucket where the intermediate files for external table will be stored."
	case "S3Prefix":
		return "A prefix for files stored in the bucket. Example: my-prefix."
	case "AWSKeyId":
		return "AWS Key ID for accessing the S3 bucket."
	case "AWSSecretKey":
		return "AWS Secret Key for accessing the S3 bucket."
	case "AWSRegion":
		return "AWS Region the bucket is in."
	default:
		return ""
	}
}

type resource struct {
	Table     string `json:"table" jsonschema:"title=Table" jsonschema_extras:"x-collection-name=true"`
	TableType string `json:"table_type" jsonschema:"title=Table Type,enum=fact,enum=dimension,default=fact"`
}

func (r resource) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing required table")
	}

	if r.TableType == "" {
		return fmt.Errorf("missing required table_type")
	}

	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Table":
		return "Name of the Firebolt table to store materialized results in. The external table will be named after this table with an `_external` suffix."
	case "TableType":
		return "Type of the Firebolt table to store materialized results in. See https://docs.firebolt.io/working-with-tables.html for more details."
	default:
		return ""
	}
}

// Removes leading and trailing slashes from prefix
func CleanPrefix(prefix string) string {
	return strings.TrimRight(strings.TrimLeft(prefix, "/"), "/")
}
