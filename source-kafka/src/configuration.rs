use anyhow::Result;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use rdkafka::ClientConfig;
use schemars::{schema::RootSchema, JsonSchema};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default)]
pub struct EndpointConfig {
    bootstrap_servers: String,
    credentials: Option<Credentials>,
    tls: Option<TlsSettings>,
    pub schema_registry: Option<SchemaRegistryConfig>,
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
    #[serde(rename = "aws")]
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
    ScramSha256,
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
}

#[derive(Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    pub endpoint: String,
    pub username: String,
    pub password: String,
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
                "credentials"
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
                                "const": "user_password"
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
                                "order": 0
                            },
                            "username": {
                                "order": 1,
                                "secret": true,
                                "title": "Username",
                                "type": "string"
                            },
                            "password": {
                                "order": 2,
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
                                "const": "AWS"
                            },
                            "aws_access_key_id": {
                                "title": "AWS Access Key ID",
                                "type": "string",
                                "order": 0
                            },
                            "aws_secret_access_key": {
                                "order": 1,
                                "secret": true,
                                "title": "AWS Secret Access Key",
                                "type": "string"
                            },
                            "region": {
                                "order": 2,
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
                        "system_certificates"
                    ],
                    "title": "TLS Settings",
                    "type": "string",
                    "order": 2
                },
                "schema_registry": {
                    "title": "Schema Registry",
                    "description": "Connection details for interacting with a schema registry. This is necessary for processing messages encoded with Avro.",
                    "type": "object",
                    "properties": {
                        "endpoint": {
                            "type": "string",
                            "title": "Schema Registry Endpoint",
                            "description": "Schema registry API endpoint. For example: https://registry-id.us-east-2.aws.confluent.cloud",
                            "order": 0 
                        },
                        "username": {
                            "type": "string",
                            "title": "Schema Registry Username",
                            "description": "Schema registry username to use for authentication. If you are using Confluent Cloud, this will be the 'Key' from your schema registry API key.",
                            "order": 1
                        },
                        "password": {
                            "type": "string",
                            "title": "Schema Registry Password",
                            "description": "Schema registry password to use for authentication. If you are using Confluent Cloud, this will be the 'Secret' from your schema registry API key.",
                            "order": 2,
                            "secret": true
                        }
                    },
                    "required": [
                        "endpoint",
                        "username",
                        "password"
                    ],
                    "order": 3
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
    pub async fn to_consumer(&self) -> Result<StreamConsumer<FlowConsumerContext>> {
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

        let consumer: StreamConsumer<FlowConsumerContext> = config.create_with_context(ctx)?;

        if let Some(Credentials::AWS { .. }) = &self.credentials {
            // In order to generate an initial OAuth Bearer token to be used by the consumer
            // we need to call poll once.
            // See https://docs.confluent.io/platform/current/clients/librdkafka/html/classRdKafka_1_1OAuthBearerTokenRefreshCb.html
            // Note that this is expected to return an error since we have no topic assignments yet
            // hence the ignoring of the result
            let _ = consumer.recv().await;
        }

        Ok(consumer)
    }

    fn security_protocol(&self) -> &'static str {
        match (&self.credentials, &self.tls) {
            (None, Some(TlsSettings::SystemCertificates)) => "SSL",
            (None, None) => "PLAINTEXT",
            (Some(_), Some(TlsSettings::SystemCertificates)) => "SASL_SSL",
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
