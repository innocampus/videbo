import aiohttp
import asyncio
import functools
import m3u8
from pathlib import Path
from typing import Dict, Optional, Set
from livestreaming.web import read_data_from_response, ResponseTooManyDataError, HTTPClient
from . import content_logger, content_settings
from shutil import rmtree as shutil_rmtree

POLL_FILES_FREQUENCY = 1 # fetch files from encoder periodically every n seconds


class StreamFetcher:
    def __init__(self, stream_id: int, encoder_base_url: str):
        self.stream_id: int = stream_id
        self.encoder_base_url: str = encoder_base_url
        self.fetcher_task: Optional[asyncio.Task] = None
        self._destroy_task: Optional[asyncio.Task] = None
        self.segments: Set[str] = set()  # all segments that are currently stored on disk
        self.current_playlist: Optional[bytes] = None # playlist file content
        self.dir: Optional[Path] = None

    def start_fetching(self):
        # Run everything in an own task.
        self.fetcher_task = asyncio.create_task(self._fetcher())

    async def _fetcher(self):
        """Periodically fetch the playlist and segments from encoder."""
        try:
            await self.create_temp_path()
            while True:
                try:
                    await self._do_fetch()
                except StreamFetchingError as error:
                    content_logger.warning(f"Error ocurred when fetching data from encoder for stream id {self.stream_id}: {error}")
                    # wait and try again, TODO: stop trying at some time if this error persists
                await asyncio.sleep(POLL_FILES_FREQUENCY)
        except asyncio.CancelledError:
            content_logger.info(f"fetch for stream {self.stream_id} was cancelled")
            pass

    def get_encoder_url(self, resource: str = ''):
        """URL to encoder"""
        return f"{self.encoder_base_url}/data/hls/{self.stream_id}/{resource}"

    def get_local_content_path(self, resource: str):
        """Get absolute path to local content node and a file within the stream directory."""
        return f"/data/hls/{self.stream_id}/{resource}"

    async def _do_fetch(self):
        try:
            # Fetch playlist.
            playlist_url = self.get_encoder_url("stream.m3u8")
            response: aiohttp.ClientResponse
            async with HTTPClient.session.get(playlist_url) as response:
                if response.status != 200:
                    raise StreamFetchingUpstreamError(f"Could not fetch playlist file, status {response.status}")

                data = await read_data_from_response(response, 512*1024)

            playlist_content = data.decode()
            await self._parse_playlist_and_fetch_segments(playlist_content)

        except (aiohttp.ClientError, UnicodeDecodeError, ResponseTooManyDataError) as error:
            raise StreamFetchingUpstreamError(str(error))

    async def _parse_playlist_and_fetch_segments(self, content: str):
        """Read the playlist and download all missing segments."""
        try:
            playlist = m3u8.loads(content, self.get_encoder_url())
            segment: m3u8.Segment
            for segment in playlist.segments:
                segment_name = segment.uri
                if segment_name not in self.segments:
                    await self._fetch_and_store_segment(segment_name)

                # Replace segment uri with the full path.
                content = content.replace(segment_name, self.get_local_content_path(segment_name))

            # Now save the playlist as bytes object, so the clients can easily fetch this file.
            self.current_playlist = content.encode()

        except ValueError:
            raise StreamFetchingError("Playlist parsing error")

    async def _fetch_and_store_segment(self, segment_name: str):
        """Fetch the segment from the encoder and store the file on disk."""
        data = None
        async with HTTPClient.session.get(self.get_encoder_url(segment_name)) as response:
            if response.status != 200:
                raise StreamFetchingUpstreamError(f"Could not segment {segment_name}, status {response.status}")

            data = await read_data_from_response(response, 10*1024*1024)

        file = Path(self.dir, segment_name)

        def save_file():
            with file.open('wb', 0) as f:
                f.write(data)
        await asyncio.get_event_loop().run_in_executor(None, save_file)
        self.segments.add(segment_name)

    async def create_temp_path(self):
        """Create a temporary directory for the stream.

        It also creates all all parent dirs if needed."""
        if self.dir:
            # As long as this exists, we assume the dir exists in the system.
            return

        self.dir = Path(content_settings.hls_temp_dir, str(self.stream_id))
        # there might be blocking io, run in another thread
        await asyncio.get_event_loop().run_in_executor(None,
                                                       functools.partial(self.dir.mkdir, parents=True, exist_ok=True))

    def destroy(self):
        async def destroyer():
            try:
                self.fetcher_task.cancel()
                await self.fetcher_task
            finally:
                rm_func = functools.partial(shutil_rmtree,
                                            path=self.dir,
                                            # function, path, error (sys.exc_info)
                                            onerror=lambda f, p, e: content_logger.error(f"{f} {p}:{e}"))
                try:
                    await asyncio.get_event_loop().run_in_executor(None, rm_func)
                except OSError as err:
                    content_logger.error(err)
                finally:
                    self.segments = []
                    self.current_playlist = None
                    stream_fetcher_collection.remove(self)
        self._destroy_task = asyncio.create_task(destroyer())


class StreamFetcherCollection:
    def __init__(self):
        self.fetcher: Dict[int, StreamFetcher] = {}

    def start_fetching_stream(self, fetcher: StreamFetcher) -> None:
        if fetcher.stream_id in self.fetcher:
            raise AlreadyFetchingStreamError()

        self.fetcher[fetcher.stream_id] = fetcher
        fetcher.start_fetching()

    def get_fetcher_by_id(self, stream_id: int) -> StreamFetcher:
        return self.fetcher[stream_id]

    def remove(self, fetcher: StreamFetcher):
        """ Expected standard case is, that <fetcher> was already fetched from collection """
        self.fetcher.pop(fetcher.stream_id)


# exceptions
class StreamFetchingError(Exception):
    pass


class StreamFetchingUpstreamError(StreamFetchingError):
    pass


class AlreadyFetchingStreamError(Exception):
    pass


stream_fetcher_collection = StreamFetcherCollection()
