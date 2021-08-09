#![allow(dead_code)]

use std::{fmt::Display, io::Read};

use schemars::JsonSchema;
use serde::{de::Visitor, Deserialize, Serialize};

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
}

impl Configuration {
    pub fn parse<R: Read>(reader: R) -> Result<Configuration, Error> {
        let configuration: Configuration = serde_json::from_reader(reader)?;

        if configuration.bootstrap_servers.is_empty() {
            return Err(Error::NoBootstrapServersGiven);
        }

        Ok(configuration)
    }

    pub fn brokers(&self) -> String {
        self.bootstrap_servers
            .iter()
            .map(|url| url.to_string())
            .collect::<Vec<String>>()
            .join(",")
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
            ]
        }"#,
        )
        .expect("to parse the config");

        let brokers = config.brokers();
        assert_eq!("localhost:9092,172.22.36.2:9093,localhost:9094", brokers);
    }
}
