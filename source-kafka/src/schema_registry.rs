use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
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
pub enum RegisteredSchema {
    Avro(apache_avro::Schema),
    Json(serde_json::Value),
    Protobuf, // TODO(whb): Protobuf support is not yet implemented.
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
            SchemaType::Protobuf => Ok(RegisteredSchema::Protobuf),
        }
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
