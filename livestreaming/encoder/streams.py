import asyncio
import functools
import shlex
import socket
import errno
from enum import Enum
from pathlib import Path
from typing import Optional
from livestreaming.streams import StreamState
from livestreaming.encoder import encoder_settings
from livestreaming.streams import Stream
from livestreaming.streams import StreamCollection
from . import logger


class PortType(Enum):
    INTERNAL = 0
    PUBLIC = 1


def get_unused_port(port_type: PortType) -> int:
    if port_type == PortType.INTERNAL:
        raw_ports = encoder_settings.rtmp_internal_ports
    else:
        raw_ports = encoder_settings.rtmp_public_ports
    for ports in raw_ports.split(","):
        port_split = ports.split("-")
        if len(port_split) > 1:
            ports = range(int(port_split[0]), int(port_split[1])+1)
        else:
            ports = [int(port_split[0])]
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("127.0.0.1", port))
                    return port
                except socket.error as e:
                    if e.errno == errno.EADDRINUSE:
                        # expected case in runtime: port may be taken
                        pass
                    else:
                        raise e
    raise PortsInUseException()


class FFmpeg:
    """
    FFmpeg and converting to HLS playlist and segments.
    """
    def __init__(self, stream: 'EncoderStream'):
        self.stream: 'EncoderStream' = stream
        self.ffmpeg_process: Optional[asyncio.subprocess.Process] = None
        self.socat_process: Optional[asyncio.subprocess.Process] = None

    async def start(self):
        program = encoder_settings.binary_ffmpeg
        url = self.stream.get_url()
        args = (f"-listen 1 -i {url} -c:v libx264 -crf 21 -preset "
                f"veryfast -c:a aac -b:a 128k -ac 2 -f hls -hls_time 3 -hls_playlist_type event stream.m3u8")

        self.ffmpeg_process = await asyncio.create_subprocess_exec(program, *shlex.split(args),
                                                                   stdout=asyncio.subprocess.DEVNULL,
                                                                   stderr=asyncio.subprocess.DEVNULL,
                                                                   cwd=self.stream.dir)

        if encoder_settings.rtmps_cert:
            args = f"openssl-listen:{self.stream.public_port},cert={encoder_settings.rtmps_cert},verify=0"
        else:
            args = f"tcp-listen:{self.stream.public_port}"
        if self.stream.ip_range:
            args += f",range={self.stream.ip_range}"
        args += f" tcp:localhost:{self.stream.port}"

        self.socat_process = await asyncio.create_subprocess_exec(encoder_settings.binary_socat, *shlex.split(args),
                                                                  stdout=asyncio.subprocess.DEVNULL,
                                                                  stderr=asyncio.subprocess.DEVNULL,
                                                                  cwd=self.stream.dir)

    async def wait(self):
        await self.ffmpeg_process.wait()
        await self.socat_process.wait()

    async def stop(self):
        for process_name in ['ffmpeg_process', 'socat_process']:
            process = getattr(self, process_name, None)
            if process:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 2)
                except asyncio.TimeoutError:
                    process.kill()
            setattr(self, process_name, None)


class EncoderStream(Stream):
    def __init__(self, stream_id: int, ip_range: Optional[str] = None):
        super().__init__(stream_id, ip_range, logger)
        self.port: int = get_unused_port(port_type=PortType.INTERNAL)
        self.public_port: int = get_unused_port(port_type=PortType.PUBLIC)
        self.control_task: Optional[asyncio.Task] = None
        self.ffmpeg: Optional[FFmpeg] = None
        self.dir: Optional[Path] = None
        self.state = StreamState.NOT_YET_STARTED

    def start(self):
        # Run everything in an own task.
        self.control_task = asyncio.create_task(self.control())

    async def control(self):
        try:
            await self.create_temp_path()
            logger.info(f"Start ffmpeg on port {self.port} for stream id {self.stream_id}")

            # Start ffmpeg.
            self.state = StreamState.WAITING_FOR_CONNECTION
            self.ffmpeg = FFmpeg(self)
            await self.ffmpeg.start()
            await self.ffmpeg.wait()
            self.state = StreamState.STOPPED

        except asyncio.CancelledError:
            if self.ffmpeg:
                await self.ffmpeg.stop()
        except Exception as err:
            logger.error(f"Error in stream controller: {str(err)}")
            self.state = StreamState.ERROR
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

    def _get_url(self, port: PortType):
        return f"rtmp://localhost:{self.public_port if port is PortType.PUBLIC else self.port}/stream"

    def get_url(self):
        return self._get_url(PortType.INTERNAL)

    def get_public_url(self):
        return self._get_url(PortType.PUBLIC)


class EncoderStreamCollection(StreamCollection[EncoderStream]):

    def create_new_stream(self, stream_id: int, ip_range: Optional[str] = None) -> EncoderStream:
        if stream_id in self.streams:
            raise StreamIdAlreadyExistsError()

        new_stream = EncoderStream(stream_id, ip_range)
        self.streams[stream_id] = new_stream
        return new_stream


class StreamIdAlreadyExistsError(Exception):
    pass


class PortsInUseException(Exception):
    pass


stream_collection = EncoderStreamCollection()
