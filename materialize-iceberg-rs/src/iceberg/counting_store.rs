use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::CopyOptions;
use object_store::GetOptions;
use object_store::GetResult;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMultipartOptions;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::Result;
use object_store::path::Path;

/// Per-request-kind counters, shared (via `Arc`) between a
/// [`CountingObjectStore`] (which increments them) and whoever logs them —
/// `transactor.rs` reads and resets them once per transaction phase, so a
/// phase's log line reports only what that phase did, not a running total
/// since connector start.
#[derive(Default)]
pub(crate) struct RequestCounts {
    get: AtomicUsize,
    get_range: AtomicUsize,
    put: AtomicUsize,
    put_multipart: AtomicUsize,
    list: AtomicUsize,
    list_with_delimiter: AtomicUsize,
    delete: AtomicUsize,
    copy: AtomicUsize,
}

impl RequestCounts {
    /// Snapshots every counter and resets it to 0, returning what was
    /// recorded since the last call (or since connector start, the first
    /// time).
    pub(crate) fn take(&self) -> RequestCountsSnapshot {
        RequestCountsSnapshot {
            get: self.get.swap(0, Ordering::Relaxed),
            get_range: self.get_range.swap(0, Ordering::Relaxed),
            put: self.put.swap(0, Ordering::Relaxed),
            put_multipart: self.put_multipart.swap(0, Ordering::Relaxed),
            list: self.list.swap(0, Ordering::Relaxed),
            list_with_delimiter: self.list_with_delimiter.swap(0, Ordering::Relaxed),
            delete: self.delete.swap(0, Ordering::Relaxed),
            copy: self.copy.swap(0, Ordering::Relaxed),
        }
    }
}

pub(crate) struct RequestCountsSnapshot {
    pub get: usize,
    pub get_range: usize,
    pub put: usize,
    pub put_multipart: usize,
    pub list: usize,
    pub list_with_delimiter: usize,
    pub delete: usize,
    pub copy: usize,
}

impl RequestCountsSnapshot {
    pub(crate) fn total(&self) -> usize {
        self.get
            + self.get_range
            + self.put
            + self.put_multipart
            + self.list
            + self.list_with_delimiter
            + self.delete
            + self.copy
    }
}

/// Wraps an [`ObjectStore`], counting requests by kind into `counts` and
/// otherwise delegating to `inner` unchanged. Overrides `get_ranges`
/// explicitly (rather than relying on its default implementation, which
/// just loops calling `get_range`) so a backend's native multi-range
/// support — and its own request count — isn't silently lost.
pub(crate) struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counts: Arc<RequestCounts>,
}

impl CountingObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, counts: Arc<RequestCounts>) -> Self {
        Self { inner, counts }
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

impl fmt::Debug for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({:?})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.counts.put.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.counts.put_multipart.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.counts.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.counts.get_range.fetch_add(1, Ordering::Relaxed);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.counts.delete.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counts.list.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counts.list_with_delimiter.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.counts.copy.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_opts(from, to, options).await
    }
}
