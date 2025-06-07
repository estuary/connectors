use anyhow::Result;
use rdkafka::{
    admin::AdminClient,
    client::{ClientContext, DefaultClientContext},
    error::KafkaError,
    producer::{DeliveryResult, ProducerContext, ThreadedProducer},
    ClientConfig,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Condvar, Mutex,
};
use schemars::{schema::RootSchema, JsonSchema};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct EndpointConfig {
    pub bootstrap_servers: String,
    pub credentials: Option<Credentials>,
    pub tls: Option<TlsSettings>,
    pub message_format: MessageFormat,
    pub compression: Compression,
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

#[derive(Serialize, Deserialize)]
pub enum MessageFormat {
    Avro,
    JSON,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Snappy => write!(f, "snappy"),
            Compression::Lz4 => write!(f, "lz4"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
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
                "compression": {
                    "description": "Compression algorithm to use for messages. Note that not all Kafka brokers support all compression algorithms.",
                    "enum": [
                        "none",
                        "gzip",
                        "lz4",
                        "snappy",
                        "zstd"
                    ],
                    "title": "Compression",
                    "type": "string",
                    "default": "lz4",
                    "order": 4
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
                    "order": 5
                },
                "topic_partitions": {
                    "title": "Topic Partitions",
                    "description": "The number of partitions to create new topics with.",
                    "type": "integer",
                    "default": 6,
                    "order": 6
                },
                "topic_replication_factor": {
                    "title": "Topic Replication Factor",
                    "description": "The replication factor to create new topics with.",
                    "type": "integer",
                    "default": 3,
                    "order": 7
                },
            }
        }))
        .unwrap()
    }
}

impl EndpointConfig {
    pub fn validate(&self) -> Result<()> {
        if matches!(self.message_format, MessageFormat::Avro) && self.schema_registry.is_none() {
            anyhow::bail!("Avro messages require a schema registry");
        }

        Ok(())
    }

    pub fn to_producer(&self) -> Result<ThreadedProducer<AckTrackingContext>> {
        let mut config = self.common_config()?;
        config.set("compression.type", self.compression.to_string());
        config.set("request.required.acks", "-1"); // Wait for all in-sync replicas
        config.set("enable.idempotence", "true");   // Ensure exactly-once semantics
        Ok(config.create_with_context(AckTrackingContext::new())?)
    }

    pub fn to_admin(&self) -> Result<AdminClient<DefaultClientContext>> {
        let config = self.common_config()?;
        Ok(config.create()?)
    }

    fn common_config(&self) -> Result<ClientConfig> {
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
            None => (),
        }

        Ok(config)
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

pub struct AckTrackingContext {
    pending_messages: AtomicUsize,
    completed_messages: AtomicUsize,
    has_failure: AtomicBool,
    state: Mutex<Option<KafkaError>>,
    notify: Condvar,
}

impl AckTrackingContext {
    pub fn new() -> Self {
        Self {
            pending_messages: AtomicUsize::new(0),
            completed_messages: AtomicUsize::new(0),
            has_failure: AtomicBool::new(false),
            state: Mutex::new(None),
            notify: Condvar::new(),
        }
    }
    
    pub fn increment_pending(&self) {
        self.pending_messages.fetch_add(1, Ordering::SeqCst);
    }
    
    pub fn wait_for_all_successful_acks(&self) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        
        loop {
            // Quick atomic check for failure (fail-fast)
            if self.has_failure.load(Ordering::SeqCst) {
                if let Some(ref error) = *guard {
                    return Err(anyhow::anyhow!("Message delivery failed: {:?}", error));
                }
            }
            
            // Check if all pending messages have been successfully delivered
            let pending = self.pending_messages.load(Ordering::SeqCst);
            let completed_count = self.completed_messages.load(Ordering::SeqCst);
            
            if pending == completed_count {
                break; // All messages successfully delivered
            }
            
            // Wait for more deliveries
            guard = self.notify.wait(guard).unwrap();
        }
        
        Ok(())
    }
}

impl ClientContext for AckTrackingContext {}

impl ProducerContext for AckTrackingContext {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult, _: Self::DeliveryOpaque) {
        match delivery_result {
            Ok(_) => {
                let completed = self.completed_messages.fetch_add(1, Ordering::SeqCst) + 1;
                let pending = self.pending_messages.load(Ordering::SeqCst);
                
                // Only notify when all messages are acknowledged
                if completed == pending {
                    self.notify.notify_all();
                }
            }
            Err((err, _)) => {
                // Always notify on failure for fail-fast behavior
                let mut state = self.state.lock().unwrap();
                if state.is_none() {
                    *state = Some(err.clone());
                    self.has_failure.store(true, Ordering::SeqCst);
                }
                self.notify.notify_all(); // Always notify on failure
            }
        }
    }
}

pub fn schema_for<T: JsonSchema>() -> RootSchema {
    schemars::gen::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}
