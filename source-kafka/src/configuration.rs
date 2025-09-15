use anyhow::Result;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::ClientConfig;
use schemars::{schema::RootSchema, JsonSchema};
use serde::{de, Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize)]
pub struct EndpointConfig {
    bootstrap_servers: String,
    credentials: Option<Credentials>,
    tls: Option<TlsSettings>,
    pub schema_registry: SchemaRegistryConfig,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "auth_type")]
#[serde(rename_all = "snake_case")]
pub enum Credentials {
    UserPassword {
        mechanism: SaslMechanism,
        username: String,
        password: String,
    },
    #[serde(rename = "AWS")]
    AWS {
        aws_access_key_id: String,
        aws_secret_access_key: String,
        region: String,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SaslMechanism {
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslMechanism::Plain => write!(f, "PLAIN"),
            SaslMechanism::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => write!(f, "SCRAM-SHA-512"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsSettings {
    SystemCertificates,
    Disabled,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "schema_registry_type")]
#[serde(rename_all = "snake_case")]
pub enum SchemaRegistryConfig {
    ConfluentSchemaRegistry {
        endpoint: String,
        username: String,
        password: String,
    },
    NoSchemaRegistry {
        #[serde(deserialize_with = "validate_json_only_true")]
        enable_json_only: bool,
    },
}

fn validate_json_only_true<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    if bool::deserialize(deserializer)? {
        Ok(true)
    } else {
        Err(de::Error::custom(
            "'enable_json_only' must be set to true when no schema registry is configured",
        ))
    }
}

impl JsonSchema for EndpointConfig {
    fn schema_name() -> String {
        "EndpointConfig".to_owned()
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Kafka Source Configuration",
            "type": "object",
            "required": [
                "bootstrap_servers",
                "credentials",
                "schema_registry"
            ],
            "properties": {
                "bootstrap_servers": {
                    "title": "Bootstrap Servers",
                    "description": "The initial servers in the Kafka cluster to initially connect to, separated by commas. The Kafka client will be informed of the rest of the cluster nodes by connecting to one of these nodes.",
                    "type": "string",
                    "order": 0
                },
                "credentials": {
                    "title": "Credentials",
                    "description": "The connection details for authenticating a client connection to Kafka via SASL. When not provided, the client connection will attempt to use PLAINTEXT (insecure) protocol. This must only be used in dev/test environments.",
                    "type": "object",
                    "order": 1,
                    "discriminator": {
                        "propertyName": "auth_type"
                    },
                    "oneOf": [{
                        "title": "SASL (User & Password)",
                        "properties": {
                            "auth_type": {
                                "type": "string",
                                "default": "user_password",
                                "const": "user_password",
                                "order": 0
                            },
                            "mechanism": {
                                "description": "The SASL Mechanism describes how to exchange and authenticate clients/servers.",
                                "enum": [
                                    "PLAIN",
                                    "SCRAM-SHA-256",
                                    "SCRAM-SHA-512"
                                ],
                                "title": "SASL Mechanism",
                                "type": "string",
                                "default": "PLAIN",
                                "order": 1
                            },
                            "username": {
                                "order": 2,
                                "title": "Username",
                                "type": "string"
                            },
                            "password": {
                                "order": 3,
                                "secret": true,
                                "title": "Password",
                                "type": "string"
                            }
                        },
                        "required": [
                            "auth_type",
                            "mechanism",
                            "password",
                            "username"
                        ]
                    }, {
                        "title": "AWS MSK IAM",
                        "properties": {
                            "auth_type": {
                                "type": "string",
                                "default": "AWS",
                                "const": "AWS",
                                "order": 0
                            },
                            "aws_access_key_id": {
                                "title": "AWS Access Key ID",
                                "type": "string",
                                "order": 1
                            },
                            "aws_secret_access_key": {
                                "order": 2,
                                "secret": true,
                                "title": "AWS Secret Access Key",
                                "type": "string"
                            },
                            "region": {
                                "order": 3,
                                "title": "AWS Region",
                                "type": "string"
                            }
                        },
                        "required": [
                            "auth_type",
                            "aws_access_key_id",
                            "aws_secret_access_key",
                            "region"
                        ]
                    }]
                },
                "tls": {
                    "default": "system_certificates",
                    "description": "Controls how should TLS certificates be found or used.",
                    "enum": [
                        "system_certificates",
                        "disabled"
                    ],
                    "title": "TLS Settings",
                    "type": "string",
                    "order": 2
                },
                "schema_registry": {
                    "title": "Schema Registry",
                    "description": "Connection details for interacting with a schema registry.",
                    "type": "object",
                    "order": 3,
                    "discriminator": {
                        "propertyName": "schema_registry_type"
                    },
                    "oneOf": [{
                        "title": "Confluent Schema Registry",
                        "properties": {
                            "schema_registry_type": {
                                "type": "string",
                                "default": "confluent_schema_registry",
                                "const": "confluent_schema_registry",
                                "order": 0
                            },
                            "endpoint": {
                                "type": "string",
                                "title": "Schema Registry Endpoint",
                                "description": "Schema registry API endpoint. For example: https://registry-id.us-east-2.aws.confluent.cloud",
                                "order": 1
                            },
                            "username": {
                                "type": "string",
                                "title": "Schema Registry Username",
                                "description": "Schema registry username to use for authentication. If you are using Confluent Cloud, this will be the 'Key' from your schema registry API key.",
                                "order": 2
                            },
                            "password": {
                                "type": "string",
                                "title": "Schema Registry Password",
                                "description": "Schema registry password to use for authentication. If you are using Confluent Cloud, this will be the 'Secret' from your schema registry API key.",
                                "order": 3,
                                "secret": true
                            }
                        },
                        "required": [
                            "endpoint",
                            "username",
                            "password"
                        ],
                    }, {
                        "title": "No Schema Registry",
                        "properties": {
                            "schema_registry_type": {
                                "type": "string",
                                "default": "no_schema_registry",
                                "const": "no_schema_registry",
                                "order": 0
                            },
                            "enable_json_only": {
                                "type": "boolean",
                                "title": "Capture Messages in JSON Format Only",
                                "description": "If no schema registry is configured the capture will attempt to parse all data as JSON, and discovered collections will use a key of the message partition & offset. All available topics will be discovered, but if their messages are not encoded as JSON attempting to capture them will result in errors. If your topics contain messages encoded with a schema, you should configure the connector to use the schema registry for optimal results.",
                                "order": 1
                            }
                        },
                        "required": [
                            "enable_json_only",
                        ],
                    }],
                }
            }
        }))
        .unwrap()
    }
}

pub struct FlowConsumerContext {
    auth: Option<Credentials>,
}

impl ClientContext for FlowConsumerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        match &self.auth {
            Some(Credentials::AWS {
                aws_access_key_id,
                aws_secret_access_key,
                region,
            }) => {
                let (token, lifetime_ms) = crate::msk_oauthbearer::token(
                    region,
                    aws_access_key_id,
                    aws_secret_access_key,
                )?;
                Ok(OAuthToken {
                    // This is just a descriptive name of the principal which is
                    // accessing the resource, not a specific constant
                    principal_name: "flow-kafka-capture".to_string(),
                    token,
                    lifetime_ms,
                })
            }
            _ => Err(anyhow::anyhow!("generate_oauth_token called without AWS credentials").into()),
        }
    }
}

impl ConsumerContext for FlowConsumerContext {}

impl EndpointConfig {
    pub async fn to_consumer(&self) -> Result<BaseConsumer<FlowConsumerContext>> {
        let mut config = ClientConfig::new();

        config.set("bootstrap.servers", self.bootstrap_servers.clone());
        config.set("enable.auto.commit", "false");
        config.set("group.id", "source-kafka"); // librdkafka will throw an error if this is left blank
        config.set("security.protocol", self.security_protocol());

        match &self.credentials {
            Some(Credentials::UserPassword {
                mechanism,
                username,
                password,
            }) => {
                config.set("sasl.mechanism", mechanism.to_string());
                config.set("sasl.username", username);
                config.set("sasl.password", password);
            }
            Some(Credentials::AWS { .. }) => {
                if self.security_protocol() != "SASL_SSL" {
                    anyhow::bail!("must use tls=system_certificates for AWS")
                }
                config.set("sasl.mechanism", "OAUTHBEARER");
            }
            None => (),
        }

        let ctx = FlowConsumerContext {
            auth: self.credentials.clone(),
        };

        let consumer: BaseConsumer<FlowConsumerContext> = config.create_with_context(ctx)?;

        if let Some(Credentials::AWS { .. }) = &self.credentials {
            // In order to generate an initial OAuth Bearer token to be used by the consumer
            // we need to call poll once.
            // See https://docs.confluent.io/platform/current/clients/librdkafka/html/classRdKafka_1_1OAuthBearerTokenRefreshCb.html
            // Note that this is expected to return an error since we have no topic assignments yet
            // hence the ignoring of the result
            let _ = consumer.poll(Some(std::time::Duration::ZERO));
        }

        Ok(consumer)
    }

    fn security_protocol(&self) -> &'static str {
        match (&self.credentials, &self.tls) {
            (None, Some(TlsSettings::SystemCertificates)) => "SSL",
            (None, Some(TlsSettings::Disabled)) => "PLAINTEXT",
            (None, None) => "PLAINTEXT",
            (Some(_), Some(TlsSettings::SystemCertificates)) => "SASL_SSL",
            (Some(_), Some(TlsSettings::Disabled)) => "SASL_PLAINTEXT",
            (Some(_), None) => "SASL_PLAINTEXT",
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Resource {
    #[schemars(title = "Topic", description = "Kafka topic to capture messages from.")]
    pub topic: String,
}

pub fn schema_for<T: JsonSchema>() -> RootSchema {
    schemars::gen::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}
