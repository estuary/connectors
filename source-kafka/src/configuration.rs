use std::{fmt::Display, io::Read};

use schemars::JsonSchema;
use serde::{de::Visitor, Deserialize, Serialize};

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

/// # Kafka Connector Configuration
#[derive(Deserialize, Default, JsonSchema, Serialize)]
pub struct Configuration {
    /// The initial servers in the Kafka cluster to initially connect to. The Kafka
    /// client will be informed of the rest of the cluster nodes by connecting to
    /// one of these nodes.
    #[schemars(with = "Vec<String>")]
    pub bootstrap_servers: Vec<BootstrapServer>,

    /// The connection details for authenticating a client connection to Kafka via SASL.
    /// When not provided, the client connection will attempt to use PLAINTEXT
    /// (insecure) protocol. This must only be used in dev/test environments.
    pub authentication: Option<Authentication>,

    /// The TLS connection settings.
    pub tls: TlsSettings,
}

impl Configuration {
    pub fn brokers(&self) -> String {
        self.bootstrap_servers
            .iter()
            .map(|url| url.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn security_protocol(&self) -> &'static str {
        match (&self.authentication, &self.tls) {
            (None, TlsSettings::SystemCertificates) => "SSL",
            (None, TlsSettings::Cleartext) => "PLAINTEXT",
            (Some(_), TlsSettings::SystemCertificates) => "SASL_SSL",
            (Some(_), TlsSettings::Cleartext) => "SASL_PLAINTEXT",
        }
    }
}

impl connector::ConnectorConfig for Configuration {
    type Error = Error;

    fn parse(reader: impl Read) -> Result<Self, Self::Error> {
        let configuration: Configuration = serde_json::from_reader(reader)?;

        if configuration.bootstrap_servers.is_empty() {
            return Err(Error::NoBootstrapServersGiven);
        }

        Ok(configuration)
    }
}

// Note: The Rust `url` crate doesn't like `ip:port` pairs without a scheme.
// Kafka doesn't specify a scheme for the `bootstrap_servers` values, so
// expecting anyone to add one is really odd. Thus, we parse these values
// ourselves.
pub struct BootstrapServer {
    host: String,
    port: u16,
}

impl BootstrapServer {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl Display for BootstrapServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", &self.host, self.port)
    }
}

impl Serialize for BootstrapServer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for BootstrapServer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(BootstrapServerVisitor)
    }
}

struct BootstrapServerVisitor;

impl<'de> Visitor<'de> for BootstrapServerVisitor {
    type Value = BootstrapServer;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a host and a port, split by a `:`")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if let Some((host, port)) = v.split_once(':') {
            if let Ok(port_num) = port.parse::<u16>() {
                Ok(BootstrapServer::new(host, port_num))
            } else {
                Err(E::custom(format!(
                    "expected the port to be a u16. got: `{}`",
                    port
                )))
            }
        } else {
            Err(E::custom(format!("expected to find a colon. got: `{}`", v)))
        }
    }
}

/// The SASL Mechanism describes _how_ to exchange and authenticate
/// clients/servers. For secure communication, TLS is **required** for all
/// supported mechanisms.
///
/// For more information about the Simple Authentication and Security Layer (SASL), see RFC 4422:
/// https://datatracker.ietf.org/doc/html/rfc4422
/// For more information about Salted Challenge Response Authentication
/// Mechanism (SCRAM), see RFC 7677.
/// https://datatracker.ietf.org/doc/html/rfc7677
#[derive(Debug, Deserialize, JsonSchema, Serialize)]
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

/// The information necessary to connect to Kafka.
#[derive(Debug, Deserialize, JsonSchema, Serialize)]
pub struct Authentication {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsSettings {
    /// Use the TLS certificates found on the system by openssl.
    SystemCertificates,
    /// Disable TLS and use an insecure connection to the Kafka brokers. This
    /// should only be used for dev/test environments.
    Cleartext,
    // TODO: allow the user to specify custom TLS certs, authorities, etc.
    // CustomCertificates(CustomTlsSettings),
}

impl Default for TlsSettings {
    fn default() -> Self {
        TlsSettings::SystemCertificates
    }
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
            "bootstrap_servers": [
                "localhost:9092",
                "172.22.36.2:9093",
                "localhost:9094"
            ],
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

        let input = std::io::Cursor::new(
            r#"
        {
            "bootstrap_servers": ["localhost:9093"],
            "tls": "system_certificates"
        }
        "#,
        );

        Configuration::parse(input).expect("to parse");

        let input = std::io::Cursor::new(
            r#"
        {
            "bootstrap_servers": ["localhost:9093"],
            "authentication": {
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "password"
            },
            "tls": "cleartext"
        }
        "#,
        );

        Configuration::parse(input).expect("to parse");
    }
}
