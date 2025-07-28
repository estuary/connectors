from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Any, Dict, List, Optional, TypeVar
from urllib.parse import quote

from pydantic import BaseModel, Field

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor


_CSVRow = TypeVar('_CSVRow', bound=BaseModel)


# Helpful docs around GCS endpoints.
# Get metadata for one object - https://cloud.google.com/storage/docs/json_api/v1/objects/get
# List metadata for objects - https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-object-glob
# Encoding URL path parts - https://cloud.google.com/storage/docs/request-endpoints#encoding


class GCSFileMetadata(BaseModel):
    """Represents metadata for a single file in Google Cloud Storage."""
    name: str = Field(..., description="The name of the object")
    bucket: str = Field(..., description="The name of the bucket containing this object")
    generation: str = Field(..., description="The content generation of this object")
    metageneration: str = Field(..., description="The metageneration of this object")
    contentType: Optional[str] = Field(None, description="Content-Type of the object data")
    timeCreated: datetime = Field(..., description="The creation time of the object")
    updated: datetime = Field(..., description="The modification time of the object")
    storageClass: str = Field(..., description="The object's storage class")
    size: str = Field(..., description="Content-Length of the data in bytes")
    md5Hash: Optional[str] = Field(None, description="MD5 hash of the data")
    crc32c: str = Field(..., description="CRC32c checksum")
    etag: str = Field(..., description="HTTP 1.1 Entity tag for the object")
    selfLink: str = Field(..., description="The link to this object")
    mediaLink: str = Field(..., description="Media download link")
    metadata: Optional[Dict[str, str]] = Field(None, description="User-provided metadata")


class GCSListResponse(BaseModel):
    """Represents a paginated response from GCS list objects API."""
    kind: str = Field(..., description="The kind of item this is")
    items: Optional[List[GCSFileMetadata]] = Field(None, description="The list of items")
    nextPageToken: Optional[str] = Field(None, description="The continuation token for pagination")
    prefixes: Optional[List[str]] = Field(None, description="The list of prefixes of objects matching-but-not-listed up to and including the requested delimiter")

# Although Google already has a Python client library for GCS, it is not asynchronous. We _could_ use asyncio.to_thread to
# run the synchronous methods in a thread, but we've seen that approach lead to performance issues in high-load scenarios.
# So we've implmented a minimal async client with only the funcionality we need.
class GCSClient:
    def __init__(self, http: HTTPSession, bucket: str, log: Logger):
        self.http = http
        self.bucket = bucket
        self.base_url = "https://storage.googleapis.com/storage/v1"
        self.log = log

    async def _fetch_files_page(
        self,
        max_results: int = 1000,
        prefix: str | None = None,
        globPattern: str | None = None,
        page_token: str | None = None,
    ) -> GCSListResponse:
        """
        List a single page of files and their metadata in a GCS bucket.
        """
        params: Dict[str, Any] = {
            "maxResults": max_results,
        }

        if prefix:
            params["prefix"] = prefix
        if globPattern:
            params["matchGlob"] = globPattern

        if page_token:
            params["pageToken"] = page_token

        url = f"{self.base_url}/b/{self.bucket}/o"

        return GCSListResponse.model_validate_json(
            await self.http.request(self.log, url, params=params)
        )

    async def list_files(
        self,
        prefix: str | None = None,
        globPattern: str | None = None,
    ) -> AsyncGenerator[GCSFileMetadata, None]:
        """
        List files in a GCS bucket. Filter which files are returned with
        the prefix and globPattern arguments. If no arguments are passed in,
        all files in the bucket are listed.
        """
        page_token: str | None = None

        while True:
            result = await self._fetch_files_page(prefix=prefix, globPattern=globPattern, page_token=page_token)

            if result.items:
                for item in result.items:
                    yield item

            if not result.nextPageToken:
                return

            page_token = result.nextPageToken

    async def stream_csv(
        self, 
        object_name: str, 
        model: type[_CSVRow],
        validation_context: object,
    ) -> AsyncGenerator[_CSVRow, None]:
        """
        Stream CSV data from a GCS object and yield parsed model instances.
        """

        encoded_object_name = quote(object_name, safe='')
        url = f"{self.base_url}/b/{self.bucket}/o/{encoded_object_name}"

        params = {"alt": "media"}

        _, body = await self.http.request_stream(
            self.log, url, params=params
        )

        processor = IncrementalCSVProcessor(
            body(),
            model,
            validation_context=validation_context
        )

        async for item in processor:
            yield item
