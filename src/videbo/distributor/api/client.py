from typing import Any

from videbo.client import Client
from videbo.storage.stored_file import StoredVideoFile
from videbo.types import HashedFileProtocol
from .models import (
    DistributorCopyFile,
    DistributorDeleteFiles,
    DistributorDeleteFilesResponse,
    DistributorFileList,
    DistributorStatus,
)


class DistributorClient(Client):
    """
    Specific `Client` subclass for interacting with the Distributor API.

    Provides convenience methods for performing common requests.
    A `DistributorClient` instance relates to one specific distributor node;
    requests via the methods introduced by this class will be sent to that
    particular node, which is identified by the `base_url` attribute.
    """

    base_url: str

    def __init__(self, node_base_url: str, **session_kwargs: Any) -> None:
        """Sets the `base_url` before initializing the client session."""
        self.base_url = node_base_url
        super().__init__(**session_kwargs)

    async def get_status(
        self,
        log_connection_error: bool = True,
    ) -> tuple[int, DistributorStatus]:
        """
        Request the current status from the distributor node.

        Returns the HTTP status code and a `DistributorStatus` object.
        """
        return await self.request(
            "GET",
            self.base_url + "/api/distributor/status",
            self.get_jwt_node(),
            return_model=DistributorStatus,
            log_connection_error=log_connection_error,
        )

    async def get_files_list(self) -> tuple[int, DistributorFileList]:
        """
        Request a list of the files that are present on the distributor node.

        Returns the HTTP status code and a `DistributorFileList` object.
        """
        return await self.request(
            "GET",
            self.base_url + "/api/distributor/files",
            self.get_jwt_node(),
            return_model=DistributorFileList,
        )

    async def copy(self, file: StoredVideoFile, *, from_url: str) -> int:
        """
        Requests copying a file to the distributor node.

        Args:
            file:
                The `StoredVideoFile` instance representing the file to upload
            from_url:
                The URL of the node that should serve as a source for the file

        Returns the HTTP status code.
        """
        code, _ = await self.request(
            "POST",
            f"{self.base_url}/api/distributor/copy/{file}",
            self.get_jwt_node(),
            data=DistributorCopyFile(
                from_base_url=from_url,
                file_size=file.size,
            ),
            timeout=30. * 60,
        )
        return code

    async def delete(
        self,
        *files: HashedFileProtocol,
        safe: bool = True,
    ) -> tuple[int, DistributorDeleteFilesResponse]:
        """
        Requests deletion of files from the distributor.

        The parameters correspond to the `DistributorDeleteFiles` model.

        Returns the HTTP status code and a
        `DistributorDeleteFilesResponse` object.
        """
        return await self.request(
            "POST",
            self.base_url + "/api/distributor/delete",
            self.get_jwt_node(),
            data=DistributorDeleteFiles.parse_obj({
                "files": files,
                "safe": safe,
            }),
            return_model=DistributorDeleteFilesResponse,
            timeout=60.,
        )
