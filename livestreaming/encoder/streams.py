import asyncio
import functools
import shlex
import string
import socket
import errno
from random import choice
from pathlib import Path
from time import time
from typing import Dict, Optional
from livestreaming.encoder import encoder_settings
from livestreaming import settings
from .api.models import StreamState
from . import logger


def get_unused_port() -> int:
    for ports in encoder_settings.rtmp_ports.split(","):
        port_split = ports.split("-")
        if len(port_split) > 1:
            ports = range(int(port_split[0]), int(port_split[1])+1)
        else:
            ports = [int(port_split[0])]
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("127.0.0.1", port))
                except socket.error as e:
                    if e.errno == errno.EADDRINUSE:
                        # expected case in runtime: port may be taken
                        pass
                    else:
                        raise e
                return port
    raise PortsInUseException()


class FFmpeg:
    """
    FFmpeg and converting to HLS playlist and segments.
    """
    def __init__(self, stream: 'Stream'):
        self.stream: 'Stream' = stream
        self.process: Optional[asyncio.subprocess.Process] = None

    async def start(self):
        program = encoder_settings.binary_ffmpeg
        url = self.stream.get_url(True)
        args = (f"-listen 1 -i {url} -c:v libx264 -crf 21 -preset "
                f"veryfast -c:a aac -b:a 128k -ac 2 -f hls -hls_time 3 -hls_playlist_type event stream.m3u8")

        self.process = await asyncio.create_subprocess_exec(program, *shlex.split(args), stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL, cwd=self.stream.dir)

    async def wait(self):
        await self.process.wait()

    async def stop(self):
        if self.process:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 2)
            except asyncio.TimeoutError:
                self.process.kill()
            self.process = None


class Stream:
    def __init__(self, stream_id: int, passw: Optional[str] = None):
        self.stream_id: int = stream_id
        self.username: str = f"user{stream_id}"
        self.port: int = get_unused_port()
        self.control_task: Optional[asyncio.Task] = None
        self.ffmpeg: Optional[FFmpeg] = None
        self.dir: Optional[Path] = None

        self.state: StreamState
        self.state_last_update: float
        self._update_state(StreamState.NOT_YET_STARTED)

        if passw:
            self.password: str = passw
        else:
            if settings.general.dev_mode:
                self.password: str = 'developer'
            else:
                base_characters = string.ascii_letters + string.digits
                self.password = ''.join(choice(base_characters) for _ in range(encoder_settings.passwd_length))

    def start(self):
        # Run everything in an own task.
        self.control_task = asyncio.create_task(self.control())

    async def control(self):
        try:
            await self.create_temp_path()
            logger.info(f"Start ffmpeg on port {self.port} for stream id {self.stream_id}")

            # Start ffmpeg.
            self._update_state(StreamState.WAITING_FOR_CONNECTION)
            self.ffmpeg = FFmpeg(self)
            await self.ffmpeg.start()
            await self.ffmpeg.wait()
            self._update_state(StreamState.STOPPED)

        except asyncio.CancelledError:
            if self.ffmpeg:
                await self.ffmpeg.stop()
        except Exception as err:
            logger.error(f"Error in stream controller: {str(err)}")
            self._update_state(StreamState.ERROR)
            if self.ffmpeg:
                await self.ffmpeg.stop()

    async def create_temp_path(self):
        """Create a temporary directory for the stream.

        It also creates all all parent dirs if needed."""
        if self.dir:
            # As long as this exists, we assume the dir exists in the system.
            return

        self.dir = Path(encoder_settings.hls_temp_dir, str(self.stream_id))
        # there might be blocking io, run in another thread
        await asyncio.get_event_loop().run_in_executor(None,
                functools.partial(self.dir.mkdir, parents=True, exist_ok=True))

    def _update_state(self, new_state: StreamState):
        self.state = new_state
        self.state_last_update = time()

    def get_url(self, with_auth: bool = False):
        auth = ''
        if with_auth:
            auth = f"{self.username}:{self.password}@"
        return f"rtmp://{auth}localhost:{self.port}/stream"


class StreamCollection:
    def __init__(self):
        self.streams: Dict[int, Stream] = {}

    def create_new_stream(self, stream_id: int) -> Stream:
        if stream_id in self.streams:
            raise StreamIdAlreadyExistsError()

        new_stream = Stream(stream_id)
        self.streams[stream_id] = new_stream
        return new_stream

    def get_stream_by_id(self, stream_id: int) -> Stream:
        return self.streams[stream_id]


class StreamIdAlreadyExistsError(Exception):
    pass


class PortsInUseException(Exception):
    pass


stream_collection = StreamCollection()
