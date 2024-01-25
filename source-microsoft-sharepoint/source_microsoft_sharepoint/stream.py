import smart_open
import base64 

from logging import Logger
from io import IOBase
from typing import Iterable, List, Optional, Any

from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from airbyte_cdk.sources.file_based.remote_file import RemoteFile as BaseRemoteFile
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec

from .client import BaseClient

class RemoteFile(BaseRemoteFile):
    download_url: str

class BaseStreamReader(AbstractFileBasedStreamReader):
    ROOT_PATH = [".", "/"]

    def __init__(self):
        super().__init__()

    @property
    def config(self) -> AbstractFileBasedSpec:
        return self._config

    @property
    def sharepoint_client(self) -> AbstractFileBasedSpec:
        return BaseClient(self._config).client

    @config.setter
    def config(self, value: AbstractFileBasedSpec):
        assert isinstance(value, AbstractFileBasedSpec)
        self._config = value

    def list_directories_and_files(self, root_folder, path=None):
        drive_items = root_folder.children.get().execute_query()
        found_items = []

        for item in drive_items:
            if item.is_file:
                found_items.append((item, str(item.name)))

        return found_items

    def get_files_by_drive_name(self, folder):
        yield from self.list_directories_and_files(folder)

    def get_matching_files(self, globs: List[str], prefix: Optional[str], logger: Logger = Logger) -> Iterable[RemoteFile]:        
        folder = self.sharepoint_client.shares.by_url(self.config.folder_share_link).drive_item.get().execute_query()
        files = self.get_files_by_drive_name(folder)
           
        try:
            first_file, path = next(files)

            yield from self.filter_files_by_globs_and_start_date(
                [
                    RemoteFile(
                        uri = path,
                        download_url = first_file.properties["@microsoft.graph.downloadUrl"],
                        last_modified = first_file.properties["lastModifiedDateTime"],
                    )
                ],
                globs,
            )

        except StopIteration as e:
            raise AirbyteTracedException(
                internal_message = str(e),
                message = f"Drive '{self.config.drive_name}' is empty or does not exist.",
                failure_type = FailureType.config_error,
                exception = e,
            )

        yield from self.filter_files_by_globs_and_start_date(
            [
                RemoteFile(
                    uri = path,
                    download_url = file.properties["@microsoft.graph.downloadUrl"],
                    last_modified = file.properties["lastModifiedDateTime"],
                )
                for file, path in files
            ],
            globs,
        )

    def open_file(self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str] = None, logger: Logger = Logger) -> IOBase:
        try:
            return smart_open.open(file.download_url, mode = mode.value, encoding = encoding)
        except Exception as e:
            logger.exception(f"Error opening file {file.uri}: {e}")
        