from datetime import datetime, timezone
from logging import Logger
from typing import AsyncGenerator, Any, Optional
from urllib.parse import parse_qsl, urlencode

from pydantic import BaseModel, Field, computed_field

from estuary_cdk.http import HeadersAndBodyGenerator, HTTPError, HTTPSession, format_error_body
from estuary_cdk.incremental_csv_processor import IncrementalCSVRowProcessor
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
        # The CDK logs a request's URL when it retries after a network error,
        # so the SAS token goes in each request's params instead of its URL.
        self._sas_params = dict(parse_qsl(sas_token, keep_blank_values=True))
        # NOTE: We may need to support dnsSuffixes other than core.windows.net.
        self.base_url = f"https://{account_name}.dfs.core.windows.net"
        self.log = log.getChild("adls_gen2_client")

    async def _request(self, url: str) -> bytes:
        try:
            return await self.http.request(self.log, url, params=self._sas_params)
        except HTTPError as err:
            raise _with_url(err, url, None) from None

    async def _request_stream(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> HeadersAndBodyGenerator:
        try:
            return await self.http.request_stream(
                self.log, url, params={**(params or {}), **self._sas_params}
            )
        except HTTPError as err:
            raise _with_url(err, url, params) from None

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

        url = f"{self.base_url}/{self.filesystem}"

        self.log.debug("Listing paths.", {
            "url": url,
            "params": params,
        })

        while True:
            headers, body = await self._request_stream(url, params)
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
        url = f"{self.base_url}/{self.filesystem}/{path}"

        self.log.debug(f"Reading file at path /{path}.", {
            "url": url,
        })

        return await self._request(url)

    async def stream_csv(
        self,
        path: str,
    ) -> AsyncGenerator[list[str], None]:
        """
        Stream and parse a CSV file, yielding each row's cells.

        Cells are left unbound to field names. Synapse Link CSVs are headerless
        and a row can be narrower than the schema describing it, so binding is
        the caller's responsibility.
        """
        url = f"{self.base_url}/{self.filesystem}/{path}"

        self.log.debug(f"Streaming CSV contents from /{path}.", {
            "url": url,
        })

        _, body = await self._request_stream(url)

        async for row in IncrementalCSVRowProcessor(body()):
            yield row


def _with_url(err: HTTPError, url: str, params: dict[str, Any] | None) -> HTTPError:
    """
    Rebuilds `err` so its message quotes `url` and `params` instead of the
    request's full URL, which includes the SAS token. Raise the result
    `from None` because a traceback also prints the error it was raised from.
    """
    if params:
        url = f"{url}?{urlencode(params)}"
    message = f"Encountered HTTP error status {err.code}.\nURL: {url}"
    if err.body is not None:
        message += f"\nResponse:\n{format_error_body(err.body)}"
    return HTTPError(message, err.code, err.body)
