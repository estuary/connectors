use anyhow::Result;
use rdkafka::admin::AdminClient;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use schemars::{schema::RootSchema, JsonSchema};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct EndpointConfig {
    pub bootstrap_servers: String,
    pub credentials: Option<Credentials>,
    pub tls: Option<TlsSettings>,
    pub message_format: MessageFormat,
    pub schema_registry: Option<SchemaRegistryConfig>,
    pub topic_partitions: i32,
    pub topic_replication_factor: i32,
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
    ScramSha256,
    ScramSha512,
}

#[derive(Serialize, Deserialize)]
pub enum MessageFormat {
    Avro,
    JSON,
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

#[derive(Serialize, Deserialize, Clone)]
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
            "title": "Kafka Materialization Configuration",
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
                                "secret": true,
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
                        "system_certificates"
                    ],
                    "title": "TLS Settings",
                    "type": "string",
                    "order": 2
                },
                "message_format": {
                    "description": "Format for materialized messages. Avro format requires a schema registry configuration. Messages in JSON format do not use a schema registry.",
                    "enum": [
                        "Avro",
                        "JSON",
                    ],
                    "title": "Message Format",
                    "type": "string",
                    "order": 3
                },
                "schema_registry": {
                    "title": "Schema Registry",
                    "description": "Connection details for interacting with a schema registry. This is necessary for materializing messages with Avro encoding.",
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
                    "order": 4
                },
                "topic_partitions": {
                    "title": "Topic Partitions",
                    "description": "The number of partitions to create new topics with.",
                    "type": "integer",
                    "default": 6,
                    "order": 5
                },
                "topic_replication_factor": {
                    "title": "Topic Replication Factor",
                    "description": "The replication factor to create new topics with.",
                    "type": "integer",
                    "default": 3,
                    "order": 6
                },
            }
        }))
        .unwrap()
    }
}

pub struct FlowClientContext {
    auth: Option<Credentials>,
}

impl ClientContext for FlowClientContext {
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

    fn error(&self, error: KafkaError, reason: &str) {
        // The default `error` implementation logs errors at the `error` log
        // level, including the expected PartitionEOF errors when reading a
        // partition to the end.
        tracing::debug!(%error, reason, "librdkafka error");
    }
}

impl ConsumerContext for FlowClientContext {}

impl EndpointConfig {
    pub fn validate(&self) -> Result<()> {
        if matches!(self.message_format, MessageFormat::Avro) && self.schema_registry.is_none() {
            anyhow::bail!("Avro messages require a schema registry");
        }

        Ok(())
    }

    pub fn to_producer(&self, transactional_id: &str) -> Result<FutureProducer<FlowClientContext>> {
        let (mut config, ctx) = self.common_config()?;
        config.set("transactional.id", transactional_id);
        config.set("compression.type", "lz4");
        Ok(config.create_with_context(ctx)?)
    }

    pub fn to_consumer(&self) -> Result<BaseConsumer<FlowClientContext>> {
        let (mut config, ctx) = self.common_config()?;

        config.set("enable.auto.commit", "false");
        // librdkafka will throw an error if this is left blank.
        config.set("group.id", "materialize-kafka");
        // This consumer is only used for restoring state from checkpoints
        // topic, and we want to know when we have reached the end of the topic.
        config.set("enable.partition.eof", "true");

        let consumer: BaseConsumer<FlowClientContext> = config.create_with_context(ctx)?;

        if let Some(Credentials::AWS { .. }) = &self.credentials {
            // In order to generate an initial OAuth Bearer token to be used by
            // the consumer we need to call poll once.
            // See https://docs.confluent.io/platform/current/clients/librdkafka/html/classRdKafka_1_1OAuthBearerTokenRefreshCb.html
            let _ = consumer.poll(Some(std::time::Duration::ZERO));
        }

        Ok(consumer)
    }

    pub fn to_admin(&self) -> Result<AdminClient<FlowClientContext>> {
        // TODO(whb): Will this generate an OAuth token without calling `poll`?
        // May not work with MSK etc.
        let (config, ctx) = self.common_config()?;
        Ok(config.create_with_context(ctx)?)
    }

    fn common_config(&self) -> Result<(ClientConfig, FlowClientContext)> {
        let mut config = ClientConfig::new();

        config.set("bootstrap.servers", self.bootstrap_servers.clone());
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

        let ctx = FlowClientContext {
            auth: self.credentials.clone(),
        };

        Ok((config, ctx))
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

#[derive(Serialize, Deserialize)]
pub struct Resource {
    pub topic: String,
}

impl JsonSchema for Resource {
    fn schema_name() -> String {
        "ResourceConfig".to_owned()
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Kafka Resource Configuration",
            "type": "object",
            "required": [
                "topic",
            ],
            "properties": {
                "topic": {
                    "title": "Topic",
                    "description": "Name of the Kafka topic to materialize to.",
                    "type": "string",
                    "x-collection-name": true
                }
            }
        }))
        .unwrap()
    }
}

pub fn schema_for<T: JsonSchema>() -> RootSchema {
    schemars::gen::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}
