from typing import Union
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
    """
    Specific `Client` subclass for interacting with the Storage API.

    Provides convenience methods for performing common requests.
    """

    async def get_status(self) -> tuple[int, StorageStatus]:
        """
        Request the current status from the storage node.

        Returns the HTTP status code and a `StorageStatus` object.
        """
        return await self.request(
            "GET",
            settings.make_url("/api/storage/status"),
            self.get_jwt_admin(),
            return_model=StorageStatus,
        )

    async def get_filtered_files(
        self,
        **kwargs: Union[str, int, bool],
    ) -> tuple[int, StorageFilesList]:
        """
        Request a list of stored files from the storage node.

        Any keyword arguments passed are encoded into the url query string,
        and may be used to filter the results.

        Returns the HTTP status code and a `StorageFilesList` object.
        """
        return await self.request(
            "GET",
            settings.make_url(f"/api/storage/files?{urlencode(kwargs)}"),
            self.get_jwt_admin(),
            return_model=StorageFilesList,
        )

    async def delete_files(self, *files: StorageFileInfo) -> tuple[int, dict[str, str]]:
        """
        Requests a batch deletion of files in storage.

        Returns the HTTP status code and a dictionary with further info.
        """
        data = DeleteFilesList(hashes=[f.hash for f in files])
        return await self.request(
            "POST",
            settings.make_url("/api/storage/delete"),
            self.get_jwt_admin(),
            data=data,
        )

    async def get_distributor_nodes(self) -> tuple[int, DistributorStatusDict]:
        """
        Request the status of active distributor nodes from the storage node.

        Returns the HTTP status code and a `DistributorStatusDict` object.
        """
        return await self.request(
            "GET",
            settings.make_url("/api/storage/distributor/status"),
            self.get_jwt_admin(),
            return_model=DistributorStatusDict,
        )

    async def set_distributor_state(self, base_url: str, enabled: bool) -> int:
        """
        Requests enabling/disabling a specific distributor node.

        Returns the HTTP status code.
        """
        prefix = "en" if enabled else "dis"
        status, _ = await self.request(
            "POST",
            settings.make_url(f"/api/storage/distributor/{prefix}able"),
            self.get_jwt_admin(),
            data=DistributorNodeInfo(base_url=base_url),
        )
        return status
