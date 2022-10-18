from typing import Optional, Union
from urllib.parse import urlencode

from videbo import settings
from videbo.client import Client
from .models import (
    DeleteFilesList,
    DistributorNodeInfo,
    DistributorStatusDict,
    StorageFileInfo,
    StorageFilesList,
    StorageStatus,
)


class StorageClient(Client):
    async def get_status(self) -> tuple[int, StorageStatus]:
        return await self.request(
            "GET",
            settings.make_url("/api/storage/status"),
            self.get_jwt_admin(),
            return_model=StorageStatus,
        )

    async def get_filtered_files(
        self,
        **kwargs: Union[str, int, bool],
    ) -> Optional[list[StorageFileInfo]]:
        """
        Makes a GET request to the storage node's files endpoint to receive a list of stored files.
        Any keyword arguments passed are encoded into the url query string, and may be used to filter the results.
        If a 200 response is received, a list of files (matching filter parameters) is returned.
        Any other response causes None to be returned.
        """
        status, ret = await self.request(
            "GET",
            settings.make_url(f"/api/storage/files?{urlencode(kwargs)}"),
            self.get_jwt_admin(),
            return_model=StorageFilesList,
        )
        if status == 200:
            return ret.files
        return None

    async def delete_files(self, *files: StorageFileInfo) -> tuple[int, dict[str, str]]:
        """
        Makes a POST request to perform a batch deletion of files in storage.
        Prints out hashes of any files that could not be deleted.
        """
        data = DeleteFilesList(hashes=[f.hash for f in files])
        return await self.request(
            "POST",
            settings.make_url("/api/storage/delete"),
            self.get_jwt_admin(),
            data=data,
        )

    async def get_distributor_nodes(self) -> tuple[int, DistributorStatusDict]:
        return await self.request(
            "GET",
            settings.make_url("/api/storage/distributor/status"),
            self.get_jwt_admin(),
            return_model=DistributorStatusDict,
        )

    async def set_distributor_state(self, base_url: str, enabled: bool) -> int:
        prefix = "en" if enabled else "dis"
        status, _ = await self.request(
            "POST",
            settings.make_url(f"/api/storage/distributor/{prefix}able"),
            self.get_jwt_admin(),
            data=DistributorNodeInfo(base_url=base_url),
        )
        return status
