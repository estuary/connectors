from datetime import datetime, timezone
from logging import Logger
from typing import AsyncGenerator, Any, Optional

from pydantic import BaseModel, Field, computed_field

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor


NEXT_PAGE_HEADER = "x-ms-continuation"


class ADLSFilesystemMetadata(BaseModel):
    """Represents metadata for a single filesystem in Azure Data Lake Storage Gen2."""
    name: str = Field(..., description="The name of the filesystem")
    eTag: str = Field(..., description="The ETag for the filesystem")
    lastModified: str = Field(..., description="The last modified time of the filesystem")


class ADLSPathMetadata(BaseModel):
    """Represents metadata for a single path in Azure Data Lake Storage Gen2."""
    name: str = Field(..., description="The name of the path")
    isDirectory: Optional[bool] = Field(None, description="True if the path is a directory")
    lastModified: str = Field(..., description="The last modified time of the path")
    etag: str = Field(..., description="The ETag for the path")
    contentLength: Optional[int] = Field(None, description="The content length in bytes for files")
    group: Optional[str] = Field(None, description="The owning group")
    owner: Optional[str] = Field(None, description="The owning user")
    permissions: Optional[str] = Field(None, description="The POSIX permissions")
    creationTime: Optional[str] = Field(None, description="Creation time as Windows file time (100-nanosecond intervals since 1601-01-01 UTC)")

    @computed_field
    @property
    def last_modified_datetime(self) -> datetime:
        """Converts lastModified GMT string (ex: 'Wed, 24 Sep 2025 14:24:24 GMT') to a datetime object."""
        return datetime.strptime(self.lastModified, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)


class ADLSFilesystemListResponse(BaseModel):
    filesystems: list[ADLSFilesystemMetadata] = Field(..., description="The list of filesystems")


class ADLSPathListResponse(BaseModel):
    paths: list[ADLSPathMetadata] = Field(..., description="The list of paths")


# This minimal async client provides only the functionality we require when listing and reading file from ADLS Gen2 Storage.
class ADLSGen2Client:
    def __init__(self, http: HTTPSession, account_name: str, filesystem: str, sas_token: str, log: Logger):
        self.http = http
        self.account_name = account_name
        self.filesystem = filesystem
        self._sas_token = sas_token
        # NOTE: We may need to support dnsSuffixes other than core.windows.net.
        self.base_url = f"https://{account_name}.dfs.core.windows.net"
        self.log = log.getChild("adls_gen2_client")

    def _build_url_with_sas(self, url: str) -> str:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}{self._sas_token}"

    async def list_paths(
        self,
        directory: str | None = None,
        recursive: bool = False,
    ) -> AsyncGenerator[ADLSPathMetadata, None]:
        params: dict[str, Any] = {
            "resource": "filesystem",
            "recursive": str(recursive).lower()
        }

        if directory:
            params["directory"] = directory

        url_without_sas = f"{self.base_url}/{self.filesystem}"

        self.log.debug("Listing paths.", {
            "url": url_without_sas,
            "params": params,
        })

        url = self._build_url_with_sas(url_without_sas)

        while True:
            headers, body = await self.http.request_stream(self.log, url, params=params)
            processor = IncrementalJsonProcessor(
                body(),
                "paths.item",
                ADLSPathMetadata,
                ADLSPathListResponse,
            )

            async for path in processor:
                yield path

            next_page_token = headers.get(NEXT_PAGE_HEADER, None)

            if not next_page_token:
                break

            params["continuation"] = next_page_token

    async def read_file(
        self,
        path: str,
    ) -> bytes:
        url_without_sas = f"{self.base_url}/{self.filesystem}/{path}"

        self.log.debug(f"Reading file at path /{path}.", {
            "url": url_without_sas,
        })

        url = self._build_url_with_sas(url_without_sas)

        return await self.http.request(self.log, url)

    async def stream_csv(
        self,
        path: str,
        field_names: list[str],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Stream and parse a CSV file, yielding dict rows.

        Transformations applied:
        - Empty strings converted to None
        """
        url_without_sas = f"{self.base_url}/{self.filesystem}/{path}"

        self.log.debug(f"Streaming CSV contents from /{path}.", {
            "url": url_without_sas,
        })

        url = self._build_url_with_sas(url_without_sas)

        _, body = await self.http.request_stream(self.log, url)

        async for row in IncrementalCSVProcessor(body(), fieldnames=field_names):
            # Convert empty strings to None
            for key, value in row.items():
                if value == "":
                    row[key] = None

            yield row
