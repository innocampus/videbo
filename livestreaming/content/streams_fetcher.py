import aiohttp
import asyncio
import functools
import m3u8
import string
import random
from pathlib import Path
from typing import Dict, Optional, Set, List
from livestreaming.web import read_data_from_response, ResponseTooManyDataError, HTTPClient
from . import content_logger, content_settings
from shutil import rmtree as shutil_rmtree

POLL_FILES_FREQUENCY = 0.3  # fetch files from encoder periodically every n seconds


class StreamFetcher:
    def __init__(self, stream_id: int, encoder_base_url: str):
        self.stream_id: int = stream_id
        self.encoder_base_url: str = encoder_base_url
        self.fetcher_task: Optional[asyncio.Task] = None
        self._destroy_task: Optional[asyncio.Task] = None
        self.main_playlist: str = ''  # the main playlist with the JWT to be replaced
        self.sub_playlists: Dict[int, SubPlaylistFetcher] = {}  # sub playlist no -> sub playlist fetcher
        self.dir: Optional[Path] = None

    def start_fetching(self):
        # Run everything in an own task.
        self.fetcher_task = asyncio.create_task(self._fetch_main_playlist_start_subtasks())

    async def _fetch_main_playlist_start_subtasks(self):
        subtasks: List[asyncio.Task] = []
        try:
            await self._create_temp_path()

            # Fetch main playlist.
            playlist_url = self.get_encoder_url("stream.m3u8")
            response: aiohttp.ClientResponse
            for current_try in range(10):
                async with HTTPClient.session.get(playlist_url) as response:
                    if response.status == 200:
                        data = await read_data_from_response(response, 512 * 1024)
                        break
                    else:
                        content_logger.warn(f"Could not fetch main playlist file for <stream {self.stream_id}>,"
                                            f"status {response.status}, retry in 1 second")
                        await asyncio.sleep(1)
            else:
                raise StreamFetchingUpstreamError(f"Could not fetch main playlist file for <stream {self.stream_id}>")

            # Read main playlist file.
            playlist_content = data.decode()
            sub_playlist_no = 0
            try:
                playlist = m3u8.loads(playlist_content)
                content_logger.info(f"Fetched main playlist for <stream {self.stream_id}>")
                subplaylist: m3u8.Playlist
                for subplaylist in playlist.playlists:
                    sub_fetcher = SubPlaylistFetcher(self, sub_playlist_no)
                    self.sub_playlists[sub_playlist_no] = sub_fetcher
                    subtasks.append(asyncio.create_task(sub_fetcher.fetch_sub_playlist_task(subplaylist.uri)))

                    # JWT needs to be replaced later.
                    new_url = self.get_local_api_path(str(sub_playlist_no)) + ".m3u8?jwt=JWT"
                    playlist_content = playlist_content.replace(subplaylist.uri, new_url)
                    sub_playlist_no += 1

                # Now save the main playlist to serve it to the clients.
                self.main_playlist = playlist_content

            except ValueError:
                raise StreamFetchingError(f"Main playlist parsing error for <stream {self.stream_id}>")

            await asyncio.gather(*subtasks)

        except asyncio.CancelledError:
            content_logger.info(f"fetch for <stream {self.stream_id}> was cancelled")
        except Exception as e:
            content_logger.exception(f"Exception while fetching playlists for <stream {self.stream_id}>")

        for task in subtasks:
            if not task.cancelled():
                task.cancel()

    def get_main_playlist(self, jwt: str) -> bytes:
        return self.main_playlist.replace("JWT", jwt).encode()

    def get_sub_playlist(self, no: int) -> bytes:
        return self.sub_playlists[no].current_playlist

    def get_encoder_url(self, resource: str = ''):
        """URL to encoder"""
        return f"{self.encoder_base_url}/{resource}"

    def get_local_api_path(self, resource: str):
        """Get absolute path to local content node and a file within the stream api dir."""
        return f"/api/content/playlist/{self.stream_id}/{resource}"

    def get_local_content_path(self, resource: str):
        """Get absolute path to local content node and a file within the stream directory."""
        return f"/data/hls/{self.stream_id}/{resource}"

    async def _create_temp_path(self):
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


class SubPlaylistFetcher:
    def __init__(self, main_fetcher: StreamFetcher, no: int):
        self.main_fetcher: StreamFetcher = main_fetcher
        self.stream_id = main_fetcher.stream_id
        self.no: int = no  # sub playlist number
        self.segments: Dict[str, str] = {}  # map old segment names to new names (that are not that easily to guess)
        self.current_playlist: bytes = b""  # bytes for faster response to client

    async def fetch_sub_playlist_task(self, sub_playlist_file: str):
        """Periodically fetch the sub playlist and segments from encoder."""
        try:
            content_logger.info(f"Start fetching sub playlist <{sub_playlist_file}> of <stream {self.stream_id}>")
            while True:
                for current_try in range(30):
                    try:
                        await self._do_fetch(sub_playlist_file)
                        break
                    except StreamFetchingError as error:
                        content_logger.warning(f"Error occurred when fetching data from encoder for <stream"
                                               f"{self.stream_id}> and sub playlist <{sub_playlist_file}>: {error}, "
                                               f"retrying")
                        # wait and try again
                        await asyncio.sleep(2 * POLL_FILES_FREQUENCY)
                else:
                    # Could not get/parse file, finally giving up.
                    raise StreamFetchingError(f"Error occurred when fetching data from encoder for <stream"
                                              f"{self.stream_id}> and sub playlist <{sub_playlist_file}>, "
                                              f"giving up")

                await asyncio.sleep(POLL_FILES_FREQUENCY)

        except asyncio.CancelledError:
            content_logger.info(f"fetch for <stream {self.stream_id}> and sub playlist <{sub_playlist_file}> was cancelled")
            pass
        except Exception as e:
            if not isinstance(e, StreamFetchingError):
                content_logger.exception(f"Exception while fetch playlists for <stream {self.stream_id}>")
            raise

    async def _do_fetch(self, sub_playlist_file: str):
        try:
            # Fetch playlist.
            playlist_url = self.main_fetcher.get_encoder_url(sub_playlist_file)
            response: aiohttp.ClientResponse
            async with HTTPClient.session.get(playlist_url) as response:
                if response.status != 200:
                    raise StreamFetchingUpstreamError(f"Could not fetch playlist file, status {response.status}")

                data = await read_data_from_response(response, 512*1024)

            playlist_content = data.decode()
            await self._parse_playlist_and_fetch_segments(sub_playlist_file, playlist_content)

        except (aiohttp.ClientError, UnicodeDecodeError, ResponseTooManyDataError) as error:
            raise StreamFetchingUpstreamError(str(error))

    async def _parse_playlist_and_fetch_segments(self, sub_playlist: str, content: str) -> None:
        """Read the playlist and download all missing segments."""
        try:
            playlist = m3u8.loads(content, self.main_fetcher.get_encoder_url())
            sequence: int = playlist.media_sequence
            segment: m3u8.Segment
            for segment in playlist.segments:
                segment_name = segment.uri
                if segment_name not in self.segments:
                    new_segment_name = await self._fetch_and_store_segment(segment_name, sequence)
                else:
                    new_segment_name = self.segments[segment_name]
                sequence += 1

                # Replace segment uri with the full path.
                content = content.replace(segment_name, self.main_fetcher.get_local_content_path(new_segment_name))

            # Now save the playlist as bytes object, so the clients can easily fetch this file.
            self.current_playlist = content.encode()

        except ValueError:
            raise StreamFetchingError("Playlist parsing error")

    async def _fetch_and_store_segment(self, segment_name: str, sequence: int) -> str:
        """Fetch the segment from the encoder and store the file on disk."""
        data = None
        async with HTTPClient.session.get(self.main_fetcher.get_encoder_url(segment_name)) as response:
            if response.status != 200:
                raise StreamFetchingUpstreamError(f"Could not segment {segment_name}, status {response.status}")

            data = await read_data_from_response(response, 10*1024*1024)

        new_name = self._get_random_segment_name(sequence)
        file = Path(self.main_fetcher.dir, new_name)

        def save_file():
            with file.open('wb', 0) as f:
                f.write(data)
        await asyncio.get_event_loop().run_in_executor(None, save_file)
        self.segments[segment_name] = new_name

        return new_name

    def _get_random_segment_name(self, sequence: int):
        rand = ''.join(random.choices(string.ascii_lowercase, k=8))
        return f"stream_{self.main_fetcher.stream_id}_{self.no}_{sequence}_{rand}.ts"


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
