package main

import (
	"fmt"
)

// TODO: make sure we have the necessary documentation information
// as requested here: https://github.com/estuary/connectors/issues/153
type config struct {
	EngineURL    string `json:"engine_url"`
	Database     string `json:"database"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	AWSKeyId     string `json:"aws_key_id,omitempty"`
	AWSSecretKey string `json:"aws_secret_key,omitempty"`
	AWSRegion    string `json:"aws_region,omitempty"`
	S3Bucket     string `json:"s3_bucket"`
	S3Prefix     string `json:"s3_prefix,omitempty"`
}

func (c config) Validate() error {
	if c.EngineURL == "" {
		return fmt.Errorf("missing required engine_url")
	}
	if c.Database == "" {
		return fmt.Errorf("missing required database")
	}
	if c.Username == "" {
		return fmt.Errorf("missing required username")
	}
	if c.Password == "" {
		return fmt.Errorf("missing required password")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("missing required bucket")
	}
	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "EngineURL":
		return "Engine URL of the Firebolt database, in the format: `<engine-name>.<organisation>.<region>.app.firebolt.io`."
	case "Database":
		return "Name of the Firebolt database."
	case "Username":
		return "Firebolt username."
	case "Password":
		return "Firebolt password."
	case "S3Bucket":
		return "Name of S3 bucket where the intermediate files for external table will be stored."
	case "S3Prefix":
		return "A prefix for files stored in the bucket."
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
	Table     string `json:"table"`
	TableType string `json:"table_type"`
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
	case "Bucket":
		return "Name of S3 bucket where the intermediate files for external table will be stored. Should be an empty S3 bucket."
	case "Prefix":
		return "A prefix for files stored in the bucket."
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
