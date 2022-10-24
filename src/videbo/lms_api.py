from __future__ import annotations
import logging
from collections.abc import AsyncIterator, Iterator
from typing import Optional, TYPE_CHECKING
from urllib.parse import urlencode

from aiohttp.web_app import Application

from videbo import settings
from videbo.client import Client
from videbo.exceptions import HTTPClientError, LMSInterfaceError
from videbo.models import (
    LMSRequestJWTData,
    VideoModel,
    VideosMissingRequest,
    VideosMissingResponse,
)

if TYPE_CHECKING:
    from videbo.storage.util import StoredHashedVideoFile as StoredFile


__all__ = ['LMS']

log = logging.getLogger(__name__)


class LMS:
    """Interface for sending requests to Learning Management Systems (LMS)"""
    FUNCTION_QUERY_PARAMETER = "function"
    VIDEOS_CHECK_MAX_BATCH_SIZE = 10_000

    _collection: dict[str, LMS] = {}

    @classmethod
    def add(cls, *urls: str) -> None:
        """Instantiates `LMS` objects with the provided `*urls`."""
        for url in urls:
            cls(url)

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        cls.add(*settings.lms_api_urls)
        yield

    @classmethod
    def iter_all(cls) -> Iterator[LMS]:
        """Returns an iterator over all `LMS` instances."""
        yield from iter(cls._collection.values())

    def __init__(self, api_url: str) -> None:
        """Adds the new instance to the class' internal collection."""
        self.api_url = api_url
        self.__class__._collection[api_url] = self

    def _get_function_url(self, function: str) -> str:
        query = urlencode({self.FUNCTION_QUERY_PARAMETER: function})
        return f"{self.api_url}?{query}"

    async def videos_missing(self, *videos: VideoModel, client: Client) -> VideosMissingResponse:
        """Checks if the provided videos are known to the LMS."""
        request_data = VideosMissingRequest(videos=list(videos))
        try:
            http_code, response_data = await client.request(
                "POST",
                self._get_function_url("videos_missing"),
                LMSRequestJWTData.get_standard_token(),
                data=request_data,
                return_model=VideosMissingResponse,
                timeout=30,
                external=True,
            )
        except HTTPClientError as e:
            raise LMSInterfaceError(
                f"Error trying to check video existence on {self.api_url}"
            ) from e
        if http_code != 200:
            raise LMSInterfaceError(
                f"Got response code {http_code} while "
                f"attempting to check video existence on {self.api_url}"
            )
        return response_data

    @classmethod
    async def filter_orphaned_videos(
        cls,
        *files: StoredFile,
        client: Client,
        origin: Optional[str] = None,
    ) -> list[VideoModel]:
        """
        Checks LMS sites for their knowledge of the provided `*files`.

        Args:
            *files:
                Any number of `StoredHashedVideoFile` objects to check for their orphan status.
                They should all be files that are actually managed by the central `FileStorage`.
            client:
                The client to use for performing requests to the LMS sites.
            origin (optional):
                If passed a string, any LMS with a matching URL is _not_ checked.

        Returns:
            List of `VideoModel` instances representing the files that are regarded as _orphaned_.
            If an `origin` was provided and that is the _only_ LMS that knows a file,
            that file will still be considered _orphaned_.

        Raises:
            `LMSInterfaceError` after logging it
        """
        video_batches: list[list[VideoModel]] = []
        for i in range(0, len(files), cls.VIDEOS_CHECK_MAX_BATCH_SIZE):
            indices = slice(i, i + cls.VIDEOS_CHECK_MAX_BATCH_SIZE)
            video_batches.append(
                [VideoModel.from_orm(file) for file in files[indices]]
            )
        for batch_idx, videos in enumerate(video_batches):
            for site in cls.iter_all():
                if origin and site.api_url.startswith(origin):
                    continue
                log.debug(f"Checking LMS {site.api_url} for {len(videos)} files.")
                try:
                    response = await site.videos_missing(*videos, client=client)
                except LMSInterfaceError as e:
                    log.warning(f"{e} occurred on {site.api_url}.")
                    raise
                # Continue only with videos unknown to previously checked LMS:
                videos = response.videos
            video_batches[batch_idx] = videos
        return [video for videos in video_batches for video in videos]
