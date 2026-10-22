use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Store {
    S3(S3Config),
    Gcs(GcsConfig),
}

#[derive(Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub credentials: Credentials,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Credentials {
    #[serde(rename = "awsAccessKeyId")]
    pub aws_access_key_id: String,
    #[serde(rename = "awsSecretAccessKey")]
    pub aws_secret_access_key: String,
}

#[derive(Serialize, Deserialize)]
pub struct GcsConfig {
    pub bucket: String,
    pub prefix: String,
    pub credentials: GcsCredentials,
}

#[derive(Serialize, Deserialize)]
pub struct GcsCredentials {
    #[serde(rename = "serviceAccountJson")]
    pub service_account_json: String,
}

#[derive(Serialize, Deserialize)]
pub struct EndpointConfig {
    pub store: Store,
}

impl JsonSchema for EndpointConfig {
    fn schema_name() -> Cow<'static, str> {
        "EndpointConfig".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value(serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Materialize Iceberg",
            "type": "object",
            "required": ["store"],
            "properties": {
                "store": {
                    "title": "Object Store",
                    "description": "The object store backing the Iceberg table's data and metadata files.",
                    "type": "object",
                    "discriminator": {
                        "propertyName": "type"
                    },
                    "oneOf": [
                        {
                            "title": "S3",
                            "description": "Store data in Amazon S3, or an S3-compatible store.",
                            "type": "object",
                            "required": ["type", "bucket", "prefix", "region", "credentials"],
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "const": "S3",
                                    "default": "S3"
                                },
                                "bucket": {
                                    "title": "Bucket",
                                    "description": "Name of the S3 bucket to write data files to.",
                                    "type": "string"
                                },
                                "prefix": {
                                    "title": "Prefix",
                                    "description": "Key prefix under the bucket to write data files under.",
                                    "type": "string"
                                },
                                "region": {
                                    "title": "Region",
                                    "description": "AWS region of the bucket.",
                                    "type": "string"
                                },
                                "credentials": {
                                    "title": "AWS Credentials",
                                    "description": "AWS credentials used to write to the bucket.",
                                    "type": "object",
                                    "required": ["awsAccessKeyId", "awsSecretAccessKey"],
                                    "properties": {
                                        "awsAccessKeyId": {
                                            "title": "AWS Access Key ID",
                                            "description": "AWS access key ID used to write to the bucket.",
                                            "type": "string"
                                        },
                                        "awsSecretAccessKey": {
                                            "title": "AWS Secret Access Key",
                                            "description": "AWS secret access key used to write to the bucket.",
                                            "type": "string",
                                            "secret": true
                                        }
                                    }
                                },
                                "endpoint": {
                                    "title": "Endpoint",
                                    "description": "Endpoint URL of an S3-compatible store to use in place of AWS S3. Only needed for testing against a local S3-compatible store.",
                                    "type": "string",
                                    "advanced": true
                                }
                            }
                        },
                        {
                            "title": "Google Cloud Storage",
                            "description": "Store data in Google Cloud Storage.",
                            "type": "object",
                            "required": ["type", "bucket", "prefix", "credentials"],
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "const": "Gcs",
                                    "default": "Gcs"
                                },
                                "bucket": {
                                    "title": "Bucket",
                                    "description": "Name of the GCS bucket to write data files to.",
                                    "type": "string"
                                },
                                "prefix": {
                                    "title": "Prefix",
                                    "description": "Key prefix under the bucket to write data files under.",
                                    "type": "string"
                                },
                                "credentials": {
                                    "title": "GCS Credentials",
                                    "description": "Google Cloud credentials used to write to the bucket.",
                                    "type": "object",
                                    "required": ["serviceAccountJson"],
                                    "properties": {
                                        "serviceAccountJson": {
                                            "title": "Service Account JSON",
                                            "description": "Contents of a Google Cloud service account key file, in JSON format.",
                                            "type": "string",
                                            "secret": true
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .unwrap()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Resource {
    pub table: String,
}

impl JsonSchema for Resource {
    fn schema_name() -> Cow<'static, str> {
        "ResourceConfig".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value(serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Iceberg Resource Configuration",
            "type": "object",
            "required": [
                "table"
            ],
            "properties": {
                "table": {
                    "title": "Table",
                    "description": "Name of the Iceberg table to materialize to.",
                    "type": "string",
                    "x-collection-name": true
                }
            }
        }))
        .unwrap()
    }
}

pub fn schema_for<T: JsonSchema>() -> schemars::Schema {
    schemars::generate::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}
