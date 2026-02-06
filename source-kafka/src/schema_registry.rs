use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use futures::stream::{self, StreamExt};
use prost_reflect::prost::Message;
use prost_reflect::DescriptorPool;
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize};
use std::collections::{HashMap, HashSet};

const TOPIC_KEY_SUFFIX: &str = "-key";
const TOPIC_VALUE_SUFFIX: &str = "-value";
const CONCURRENT_SCHEMA_REQUESTS: usize = 10;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FetchedSchema {
    #[serde(default = "SchemaType::default")]
    schema_type: SchemaType,
    schema: String,
    references: Option<serde_json::Value>, // TODO(whb): Schema reference support is not yet implemented.
}

#[derive(Deserialize, Debug)]
struct FetchedLatestVersion {
    id: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    fn default() -> Self {
        SchemaType::Avro
    }
}

#[derive(Debug)]
pub struct ProtobufSchema {
    pub descriptor_pool: DescriptorPool,
    pub message_name: String,
}

#[derive(Debug)]
pub enum RegisteredSchema {
    Avro(apache_avro::Schema),
    Json(serde_json::Value),
    Protobuf(ProtobufSchema),
}

#[derive(Debug, Default)]
pub struct TopicSchema {
    pub key: Option<RegisteredSchema>,
    pub value: Option<RegisteredSchema>,
}

pub struct SchemaRegistryClient {
    endpoint: String,
    http: Client,
    username: String,
    password: String,
}

impl SchemaRegistryClient {
    pub fn new(endpoint: String, username: String, password: String) -> SchemaRegistryClient {
        SchemaRegistryClient {
            endpoint: endpoint.to_string(),
            http: reqwest::Client::default(),
            username,
            password,
        }
    }

    pub async fn schemas_for_topics(
        &self,
        topics: &[String],
    ) -> Result<HashMap<String, TopicSchema>> {
        let applicable_topics: HashSet<String> = topics.iter().cloned().collect();

        let subjects: Vec<String> = self
            .make_request(format!("{}/subjects", self.endpoint).as_str())
            .await?;

        let filter_by_suffix = |s: &str, suffix: &str| {
            if let Some(s) = s.strip_suffix(suffix) {
                if !applicable_topics.contains(s) {
                    return None;
                }
                return Some(s.to_string());
            }
            None
        };

        let topics_with_key_schema: HashSet<String> = subjects
            .iter()
            .filter_map(|s| filter_by_suffix(s, TOPIC_KEY_SUFFIX))
            .collect();

        let topics_with_value_schema: HashSet<String> = subjects
            .iter()
            .filter_map(|s| filter_by_suffix(s, TOPIC_VALUE_SUFFIX))
            .collect();

        let schema_futures: Vec<_> = applicable_topics
            .iter()
            .filter_map(|topic| {
                let need_key = topics_with_key_schema.contains(topic);
                let need_value = topics_with_value_schema.contains(topic);
                if !need_key && !need_value {
                    return None;
                }
                Some(async move {
                    let mut schema = TopicSchema {
                        key: None,
                        value: None,
                    };

                    if need_key {
                        schema.key = Some(self.fetch_latest_schema(topic, true).await?)
                    }
                    if need_value {
                        schema.value = Some(self.fetch_latest_schema(topic, false).await?)
                    }

                    Ok::<(String, TopicSchema), anyhow::Error>((topic.to_owned(), schema))
                })
            })
            .collect();

        stream::iter(schema_futures)
            .buffer_unordered(CONCURRENT_SCHEMA_REQUESTS)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashMap<String, TopicSchema>>>()
    }

    pub async fn fetch_schema(&self, id: u32) -> Result<RegisteredSchema> {
        let fetched: FetchedSchema = self
            .make_request(format!("{}/schemas/ids/{}", self.endpoint, id).as_str())
            .await?;

        if fetched.references.is_some() {
            anyhow::bail!("schema references are not yet supported, and requested schema with id {} has references", id);
        }

        match fetched.schema_type {
            SchemaType::Avro => {
                let schema = apache_avro::Schema::parse_str(&fetched.schema)
                    .context("failed to parse fetched avro schema")?;
                Ok(RegisteredSchema::Avro(schema))
            }
            SchemaType::Json => {
                let schema = serde_json::from_str(&fetched.schema)
                    .context("failed to parse fetched json schema")?;
                Ok(RegisteredSchema::Json(schema))
            }
            SchemaType::Protobuf => {
                self.fetch_protobuf_schema(id, &fetched.schema).await
            }
        }
    }

    async fn fetch_protobuf_schema(&self, id: u32, schema_text: &str) -> Result<RegisteredSchema> {
        // Fetch the schema with serialized format to get the FileDescriptorProto bytes
        // When format=serialized is used, the schema field contains base64-encoded FileDescriptorProto
        let fetched: FetchedSchema = self
            .make_request(format!("{}/schemas/ids/{}?format=serialized", self.endpoint, id).as_str())
            .await?;

        if fetched.references.is_some() {
            anyhow::bail!("schema references are not yet supported, and requested protobuf schema with id {} has references", id);
        }

        // With format=serialized, the schema field contains base64-encoded FileDescriptorProto
        let decoded_bytes = base64.decode(&fetched.schema)
            .context("failed to decode base64 schema from serialized format")?;

        // The schema registry returns a FileDescriptorProto, not a FileDescriptorSet
        // We need to wrap it in a FileDescriptorSet for prost-reflect
        let file_descriptor_proto = prost_types::FileDescriptorProto::decode(decoded_bytes.as_slice())
            .context("failed to decode FileDescriptorProto from schema bytes")?;

        let file_descriptor_set = prost_types::FileDescriptorSet {
            file: vec![file_descriptor_proto],
        };

        let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .context("failed to create DescriptorPool from FileDescriptorSet")?;

        // Extract the message name from the original schema text.
        // Protobuf schema text format has the message name as the first message definition.
        let message_name = extract_protobuf_message_name(schema_text)
            .context("failed to extract message name from protobuf schema")?;

        Ok(RegisteredSchema::Protobuf(ProtobufSchema {
            descriptor_pool,
            message_name,
        }))
    }

    async fn fetch_latest_version(&self, subject: &str) -> Result<u32> {
        let fetched: FetchedLatestVersion = self
            .make_request(
                format!("{}/subjects/{}/versions/latest", self.endpoint, subject).as_str(),
            )
            .await?;
        Ok(fetched.id)
    }

    async fn fetch_latest_schema(&self, topic: &str, key: bool) -> Result<RegisteredSchema> {
        let subject = format!(
            "{}{}",
            topic,
            if key {
                TOPIC_KEY_SUFFIX
            } else {
                TOPIC_VALUE_SUFFIX
            }
        );
        let version = self.fetch_latest_version(subject.as_str()).await?;
        self.fetch_schema(version).await
    }

    async fn make_request<T>(&self, url: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let res = self
            .http
            .get(url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;

        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await?;
            anyhow::bail!(
                "request GET {} failed with status {}: {}",
                url,
                status,
                body
            );
        }

        Ok(res.json().await?)
    }
}

/// Extracts the first message name from a protobuf schema text.
/// The schema text is expected to be in standard .proto format.
/// Returns the fully-qualified message name (with package prefix if present).
fn extract_protobuf_message_name(schema_text: &str) -> Option<String> {
    let mut package = String::new();
    let mut in_message = false;
    let mut brace_depth = 0;

    for line in schema_text.lines() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        // Extract package name
        if line.starts_with("package ") {
            let pkg = line
                .trim_start_matches("package ")
                .trim_end_matches(';')
                .trim();
            package = pkg.to_string();
            continue;
        }

        // Track brace depth
        brace_depth += line.chars().filter(|&c| c == '{').count() as i32;
        brace_depth -= line.chars().filter(|&c| c == '}').count() as i32;

        // Look for top-level message definition (at brace_depth 0 before the line is processed)
        if !in_message && brace_depth <= 1 && line.starts_with("message ") {
            let name_part = line.trim_start_matches("message ");
            let name = name_part
                .split(|c: char| c.is_whitespace() || c == '{')
                .next()?
                .trim();

            if package.is_empty() {
                return Some(name.to_string());
            } else {
                return Some(format!("{}.{}", package, name));
            }
        }

        if line.starts_with("message ") {
            in_message = true;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_protobuf_message_name() {
        // Simple message without package
        let schema1 = r#"
            syntax = "proto3";
            message TestMessage {
                string field1 = 1;
            }
        "#;
        assert_eq!(
            extract_protobuf_message_name(schema1),
            Some("TestMessage".to_string())
        );

        // Message with package
        let schema2 = r#"
            syntax = "proto3";
            package com.example.test;
            message MyRecord {
                int32 id = 1;
                string name = 2;
            }
        "#;
        assert_eq!(
            extract_protobuf_message_name(schema2),
            Some("com.example.test.MyRecord".to_string())
        );

        // Message with nested message (should return top-level)
        let schema3 = r#"
            syntax = "proto3";
            package mypackage;
            message OuterMessage {
                message InnerMessage {
                    string value = 1;
                }
                InnerMessage inner = 1;
            }
        "#;
        assert_eq!(
            extract_protobuf_message_name(schema3),
            Some("mypackage.OuterMessage".to_string())
        );

        // With comments
        let schema4 = r#"
            syntax = "proto3";
            // This is a comment
            package test;
            // Another comment
            message CommentedMessage {
                string field = 1;
            }
        "#;
        assert_eq!(
            extract_protobuf_message_name(schema4),
            Some("test.CommentedMessage".to_string())
        );
    }
}
