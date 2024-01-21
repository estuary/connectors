import smart_open

from logging import Logger
from io import IOBase
from typing import Iterable, List, Optional, Any

from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile as BaseRemoteFile
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec

from .client import BaseClient

class RemoteFile(BaseRemoteFile):
    name: str
    type: str
    download_url: str

class BaseStreamReader(AbstractFileBasedStreamReader):
    ROOT_PATH = [".", "/"]

    def __init__(self):
        super().__init__()

    @property
    def config(self) -> AbstractFileBasedSpec:
        return self._config

    @property
    def one_drive_client(self) -> AbstractFileBasedSpec:
        return BaseClient(self._config).client

    @config.setter
    def config(self, value: AbstractFileBasedSpec):
        assert isinstance(value, AbstractFileBasedSpec)
        self._config = value

    def list_directories_and_files(self, root_folder, path=None):
        drive_items = root_folder.children.get().execute_query()
        found_items = []
        for item in drive_items:
            item_path = path + "/" + item.name if path else item.name
            if item.is_file:
                found_items.append((item, item_path))
            else:
                found_items.extend(self.list_directories_and_files(item, item_path))
        return found_items

    def get_files_by_drive_name(self, drives, drive_name, folder_path):
        path_levels = [level for level in folder_path.split("/") if level]
        folder_path = "/".join(path_levels)

        for drive in drives:
            is_onedrive = drive.drive_type in ["personal", "business"]
            if drive.name == drive_name and is_onedrive:
                folder = drive.root if folder_path in self.ROOT_PATH else drive.root.get_by_path(folder_path).get().execute_query()
                yield from self.list_directories_and_files(folder)

    def get_matching_files(self, globs: List[str], prefix: Optional[str], logger: Logger = Logger) -> Iterable[RemoteFile]:
        drives = self.one_drive_client.drives.get().execute_query()
        self.config.drive_name = None

        if self.config.credentials.auth_type == "Client":
            my_drive = self.one_drive_client.me.drive.get().execute_query()

        drives.add_child(my_drive)

        files = self.get_files_by_drive_name(drives, self.config.drive_name, self.config.folder_path)
        yield from self.filter_files_by_globs_and_start_date(
            [
                RemoteFile(
                    name = file.properties["name"],
                    type = file.properties["name"].split(".")[-1],
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
        