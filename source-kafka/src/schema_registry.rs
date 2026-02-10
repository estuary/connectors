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

#[derive(Deserialize, Debug, Clone)]
struct SchemaReference {
    name: String,    // import path, e.g. "common/types.proto"
    subject: String, // Schema Registry subject
    version: u32,    // version of the referenced schema
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FetchedSchema {
    #[serde(default = "SchemaType::default")]
    schema_type: SchemaType,
    schema: String,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

#[derive(Deserialize, Debug)]
struct FetchedSubjectVersion {
    id: u32,
    #[allow(dead_code)]
    schema: String,
    #[serde(default)]
    references: Vec<SchemaReference>,
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

        match fetched.schema_type {
            SchemaType::Avro => {
                if !fetched.references.is_empty() {
                    anyhow::bail!("schema references are not yet supported for Avro schemas (schema id {})", id);
                }
                let schema = apache_avro::Schema::parse_str(&fetched.schema)
                    .context("failed to parse fetched avro schema")?;
                Ok(RegisteredSchema::Avro(schema))
            }
            SchemaType::Json => {
                if !fetched.references.is_empty() {
                    anyhow::bail!("schema references are not yet supported for JSON schemas (schema id {})", id);
                }
                let schema = serde_json::from_str(&fetched.schema)
                    .context("failed to parse fetched json schema")?;
                Ok(RegisteredSchema::Json(schema))
            }
            SchemaType::Protobuf => {
                self.fetch_protobuf_schema(id, &fetched.references).await
            }
        }
    }

    async fn fetch_protobuf_schema(
        &self,
        id: u32,
        references: &[SchemaReference],
    ) -> Result<RegisteredSchema> {
        // Resolve all referenced schemas first (depth-first for topological order).
        let dependency_descriptors = self.resolve_protobuf_references(references).await?;

        // Fetch the main schema with serialized format to get FileDescriptorProto bytes.
        let fetched: FetchedSchema = self
            .make_request(
                format!("{}/schemas/ids/{}?format=serialized", self.endpoint, id).as_str(),
            )
            .await?;

        let decoded_bytes = base64
            .decode(&fetched.schema)
            .context("failed to decode base64 schema from serialized format")?;

        let main_descriptor = prost_types::FileDescriptorProto::decode(decoded_bytes.as_slice())
            .context("failed to decode FileDescriptorProto from schema bytes")?;

        // Extract the message name from the FileDescriptorProto directly.
        let package = main_descriptor.package.as_deref().unwrap_or("");
        let first_message = main_descriptor
            .message_type
            .first()
            .context("no message types found in protobuf schema")?;
        let name = first_message
            .name
            .as_deref()
            .context("message type has no name in protobuf schema")?;
        let message_name = if package.is_empty() {
            name.to_string()
        } else {
            format!("{}.{}", package, name)
        };

        // Build FileDescriptorSet with dependencies first, then the main schema.
        let mut files = dependency_descriptors;
        files.push(main_descriptor);

        let file_descriptor_set = prost_types::FileDescriptorSet { file: files };

        let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .context("failed to create DescriptorPool from FileDescriptorSet")?;

        Ok(RegisteredSchema::Protobuf(ProtobufSchema {
            descriptor_pool,
            message_name,
        }))
    }

    /// Recursively resolves all protobuf schema references from the Schema Registry.
    /// Returns FileDescriptorProtos in dependency-first (topological) order.
    async fn resolve_protobuf_references(
        &self,
        references: &[SchemaReference],
    ) -> Result<Vec<prost_types::FileDescriptorProto>> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        self.resolve_protobuf_references_inner(references, &mut result, &mut visited)
            .await?;
        Ok(result)
    }

    async fn resolve_protobuf_references_inner(
        &self,
        references: &[SchemaReference],
        result: &mut Vec<prost_types::FileDescriptorProto>,
        visited: &mut HashSet<String>,
    ) -> Result<()> {
        for reference in references {
            // prost-reflect has google well-known types built in.
            if reference.name.starts_with("google/protobuf/") {
                continue;
            }

            let visit_key = format!("{}:{}", reference.subject, reference.version);
            if !visited.insert(visit_key) {
                continue;
            }

            // Fetch the referenced schema by subject+version to get its ID and nested references.
            let subject_version: FetchedSubjectVersion = self
                .make_request(
                    format!(
                        "{}/subjects/{}/versions/{}",
                        self.endpoint, reference.subject, reference.version
                    )
                    .as_str(),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch referenced schema subject={} version={}",
                        reference.subject, reference.version
                    )
                })?;

            // Recurse into nested references first (depth-first = topological order).
            Box::pin(self.resolve_protobuf_references_inner(
                &subject_version.references,
                result,
                visited,
            ))
            .await?;

            // Fetch the serialized FileDescriptorProto for this reference.
            let fetched: FetchedSchema = self
                .make_request(
                    format!(
                        "{}/schemas/ids/{}?format=serialized",
                        self.endpoint, subject_version.id
                    )
                    .as_str(),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch serialized schema for reference {} (id={})",
                        reference.name, subject_version.id
                    )
                })?;

            let decoded_bytes = base64.decode(&fetched.schema).with_context(|| {
                format!(
                    "failed to decode base64 schema for reference {}",
                    reference.name
                )
            })?;

            let mut file_descriptor_proto =
                prost_types::FileDescriptorProto::decode(decoded_bytes.as_slice()).with_context(
                    || {
                        format!(
                            "failed to decode FileDescriptorProto for reference {}",
                            reference.name
                        )
                    },
                )?;

            // Override the name to match the import path so prost-reflect resolves dependencies.
            file_descriptor_proto.name = Some(reference.name.clone());

            result.push(file_descriptor_proto);
        }

        Ok(())
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

