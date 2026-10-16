use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::snapshot::Snapshot;
use super::table_metadata::MetadataLogEntry;
use super::table_metadata::TableMetadata;

/// Path segment identifying this catalog instance in the REST API, e.g.
/// `POST /v1/{CATALOG_PREFIX}/transactions/commit`. There's only ever one
/// catalog per connector instance, so this is a fixed literal rather than
/// something clients discover via `GET /v1/config`.
pub const CATALOG_PREFIX: &str = "catalog";

/// Request body for `POST /v1/{prefix}/transactions/commit`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<TableChange>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableChange {
    pub identifier: TableIdentifier,
    #[serde(default)]
    pub updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableIdentifier {
    pub name: String,
}

/// The subset of Iceberg's REST table-update actions this catalog
/// understands. `add-schema` only has an effect the first time a table is
/// created — schema evolution isn't supported, so later commits simply carry
/// it along unused.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AddSchema {
        schema: Value,
        last_column_id: i32,
    },
    AddSnapshot {
        snapshot: Snapshot,
    },
    SetSnapshotRef {
        ref_name: String,
        #[serde(rename = "type")]
        ref_type: String,
        snapshot_id: i64,
    },
}

/// The connector's embedded REST catalog state: an object-store-backed set
/// of Iceberg tables, one `metadata.json` per table. There's no external
/// catalog database, so the current metadata file for a table is located via
/// a `version-hint.text` pointer file, following the classic Hadoop-catalog
/// convention.
pub struct CatalogState {
    pub object_store: Arc<dyn ObjectStore>,
    pub prefix: String,
    pub bucket: String,
}

impl CatalogState {
    fn version_hint_path(&self, table: &str) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/{}/metadata/version-hint.text",
            self.prefix, table
        ))
    }

    fn metadata_dir(&self, table: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{}/metadata", self.prefix, table))
    }

    /// Builds the path for a newly-written `metadata.json`, following the
    /// `<version>-<uuid>.metadata.json` convention (zero-padded to 5 digits)
    /// used by other Iceberg implementations, rather than the simpler
    /// `v<version>.metadata.json` form — so that readers which scan the
    /// metadata directory for the highest version (rather than trusting
    /// `version-hint.text`) see the same naming they'd see from any other
    /// writer.
    fn new_metadata_path(&self, table: &str, version: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/{}/metadata/{:05}-{}.metadata.json",
            self.prefix,
            table,
            version,
            Uuid::new_v4()
        ))
    }

    /// Finds the metadata file for `version` by listing the table's metadata
    /// directory, since its filename carries a UUID that can't be derived
    /// from the version number alone.
    async fn find_metadata_path(&self, table: &str, version: u64) -> Result<ObjectPath> {
        let listing = self
            .object_store
            .list_with_delimiter(Some(&self.metadata_dir(table)))
            .await
            .context("listing table metadata directory")?;

        let version_prefix = format!("{version:05}-");
        listing
            .objects
            .into_iter()
            .map(|meta| meta.location)
            .find(|path| {
                path.filename()
                    .is_some_and(|name| name.starts_with(&version_prefix))
            })
            .with_context(|| format!("no metadata file found for version {version}"))
    }

    async fn load_metadata(&self, table: &str) -> Result<Option<(u64, TableMetadata)>> {
        let version = match self.object_store.get(&self.version_hint_path(table)).await {
            Ok(result) => {
                let bytes = result.bytes().await.context("reading version-hint")?;
                std::str::from_utf8(&bytes)
                    .context("version-hint is not UTF-8")?
                    .trim()
                    .parse::<u64>()
                    .context("version-hint does not contain a number")?
            }
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let metadata_path = self.find_metadata_path(table, version).await?;

        let bytes = self
            .object_store
            .get(&metadata_path)
            .await
            .context("fetching table metadata")?
            .bytes()
            .await
            .context("reading table metadata body")?;

        let metadata: TableMetadata =
            serde_json::from_slice(&bytes).context("decoding table metadata")?;
        Ok(Some((version, metadata)))
    }

    /// Applies a batch of table updates, creating the table's initial
    /// metadata if it doesn't exist yet, and writes the resulting
    /// `metadata.json` plus an updated `version-hint.text`.
    pub async fn commit(&self, table: &str, updates: &[TableUpdate]) -> Result<TableMetadata> {
        let mut schema = None;
        let mut last_column_id = 0;
        let mut snapshot = None;

        for update in updates {
            match update {
                TableUpdate::AddSchema {
                    schema: s,
                    last_column_id: lcid,
                } => {
                    schema = Some(s.clone());
                    last_column_id = *lcid;
                }
                TableUpdate::AddSnapshot { snapshot: s } => snapshot = Some(s.clone()),
                TableUpdate::SetSnapshotRef { .. } => {}
            }
        }

        let existing = self.load_metadata(table).await?;
        let version = existing.as_ref().map_or(0, |(version, _)| *version);

        let mut metadata = match existing {
            Some((_, metadata)) => metadata,
            None => {
                let schema =
                    schema.context("creating a new table requires an add-schema update")?;
                TableMetadata::new(
                    format!("s3://{}/{}/{}", self.bucket, self.prefix, table),
                    schema,
                    last_column_id,
                )
            }
        };

        let snapshot = snapshot.context("commit requires an add-snapshot update")?;

        let next_version = version + 1;
        let metadata_path = self.new_metadata_path(table, next_version);
        let metadata_file = format!("s3://{}/{}", self.bucket, metadata_path);
        metadata.add_snapshot(snapshot);
        metadata.metadata_log.push(MetadataLogEntry {
            timestamp_ms: metadata.last_updated_ms,
            metadata_file,
        });

        let bytes = serde_json::to_vec_pretty(&metadata).context("encoding table metadata")?;
        self.object_store
            .put(&metadata_path, PutPayload::from(bytes))
            .await
            .context("uploading table metadata")?;
        self.object_store
            .put(
                &self.version_hint_path(table),
                PutPayload::from(next_version.to_string().into_bytes()),
            )
            .await
            .context("updating version-hint")?;

        Ok(metadata)
    }
}
