use std::fmt::Display;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::connector;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read the configuration file")]
    File(#[from] std::io::Error),

    #[error("failed to parse the file as valid json")]
    Parsing(#[from] serde_json::Error),

    #[error("bootstrap servers are required to make the initial connection")]
    NoBootstrapServersGiven,
}

/// # Kafka Source Configuration
#[derive(Deserialize, Default, Debug, Serialize)]
pub struct Configuration {
    /// # Bootstrap Servers
    ///
    /// The initial servers in the Kafka cluster to initially connect to. The Kafka
    /// client will be informed of the rest of the cluster nodes by connecting to
    /// one of these nodes.
    pub bootstrap_servers: String,

    /// # Credentials
    ///
    /// The connection details for authenticating a client connection to Kafka via SASL.
    /// When not provided, the client connection will attempt to use PLAINTEXT
    /// (insecure) protocol. This must only be used in dev/test environments.
    pub credentials: Option<Credentials>,

    /// # TLS connection settings.
    pub tls: Option<TlsSettings>,
}

impl Configuration {
    pub fn brokers(&self) -> String {
        self.bootstrap_servers.clone()
    }

    pub fn security_protocol(&self) -> &'static str {
        match (&self.credentials, &self.tls) {
            (None, Some(TlsSettings::SystemCertificates)) => "SSL",
            (None, None) => "PLAINTEXT",
            (Some(_), Some(TlsSettings::SystemCertificates)) => "SASL_SSL",
            (Some(_), None) => "SASL_PLAINTEXT",
        }
    }
}

impl JsonSchema for Configuration {
    fn schema_name() -> String {
        "Configuration".to_owned()
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
                                "default": "UserPassword",
                                "const": "UserPassword"
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
                            "password": {
                                "order": 2,
                                "secret": true,
                                "title": "Password",
                                "type": "string"
                            },
                            "username": {
                                "order": 1,
                                "secret": true,
                                "title": "Username",
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
                "bootstrap_servers": {
                    "title": "Bootstrap Servers",
                    "description": "The initial servers in the Kafka cluster to initially connect to, separated by commas. The Kafka client will be informed of the rest of the cluster nodes by connecting to one of these nodes.",
                    "type": "string",
                    "order": 0
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
                }
            }
        }))
        .unwrap()
    }
}

impl connector::ConnectorConfig for Configuration {
    type Error = Error;

    fn parse(reader: &str) -> Result<Self, Self::Error> {
        let configuration: Configuration = serde_json::from_str(reader)?;

        if configuration.bootstrap_servers.is_empty() {
            return Err(Error::NoBootstrapServersGiven);
        }

        Ok(configuration)
    }
}

/// # SASL Mechanism
///
/// The SASL Mechanism describes _how_ to exchange and authenticate
/// clients/servers. For secure communication, TLS is **required** for all
/// supported mechanisms.
///
/// For more information about the Simple Authentication and Security Layer (SASL), see RFC 4422:
/// https://datatracker.ietf.org/doc/html/rfc4422
/// For more information about Salted Challenge Response Authentication
/// Mechanism (SCRAM), see RFC 7677.
/// https://datatracker.ietf.org/doc/html/rfc7677
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SaslMechanism {
    /// The username and password are sent to the server in the clear.
    Plain,
    /// SCRAM using SHA-256.
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    /// SCRAM using SHA-512.
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslMechanism::Plain => write!(f, "PLAIN"),
            SaslMechanism::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => write!(f, "SCRAM-SHA-512"),
        }
    }
}

/// # Credentials
///
/// The information necessary to connect to Kafka.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag="auth_type")]
pub enum Credentials {
    UserPassword {
        /// # Sasl Mechanism
        mechanism: SaslMechanism,
        /// # Username
        username: String,
        /// # Password
        password: String,
    },

    AWS {
        #[serde(rename="aws_access_key_id")]
        access_key_id: String,
        #[serde(rename="aws_secret_access_key")]
        secret_access_key: String,
        region: String,
    }
}

/// # TLS Settings
///
/// Controls how should TLS certificates be found or used.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum TlsSettings {
    /// Use the TLS certificates bundled with openssl.
    #[default]
    SystemCertificates,
    // TODO: allow the user to specify custom TLS certs, authorities, etc.
    // CustomCertificates(CustomTlsSettings),
}



#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty_brokers_test() {
        let config = Configuration::default();
        let brokers = config.brokers();
        assert_eq!("", brokers);
    }

    #[test]
    fn many_brokers_test() {
        let config: Configuration = serde_json::from_str(
            r#"{
            "bootstrap_servers": "localhost:9092,172.22.36.2:9093,localhost:9094",
            "tls": "system_certificates"
        }"#,
        )
        .expect("to parse the config");

        let brokers = config.brokers();
        assert_eq!("localhost:9092,172.22.36.2:9093,localhost:9094", brokers);
    }

    #[test]
    fn parse_config_file_test() {
        use connector::ConnectorConfig;

        let input = r#"
        {
            "bootstrap_servers": "localhost:9093",
            "tls": "system_certificates"
        }
        "#;

        Configuration::parse(input).expect("to parse");

        let input = r#"
        {
            "bootstrap_servers": "localhost:9093",
            "credentials": {
                "auth_type": "UserPassword",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "password"
            },
            "tls": null
        }
        "#;

        Configuration::parse(input).expect("to parse");
    }
}
