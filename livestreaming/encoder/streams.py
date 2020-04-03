import asyncio
import functools
import random
import shlex
import socket
import string
import errno
import re
from enum import Enum
from pathlib import Path
from typing import Optional
from livestreaming.streams import StreamState
from livestreaming.encoder import encoder_settings
from livestreaming.streams import Stream
from livestreaming.streams import StreamCollection
from livestreaming.encoder.api.models import StreamRecordingStartParams
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
                    # if port is taken exception will raise
                    return port
                except socket.error as e:
                    if e.errno == errno.EADDRINUSE:
                        # expected case at runtime
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
        self.watch_task: Optional[asyncio.Task] = None
        self.user: Optional[str] = encoder_settings.ffmpeg_user

    async def start(self):
        program = encoder_settings.binary_ffmpeg
        url = self.stream.get_url()

        video_split = "split=2 [vtemp001][vtemp002]"
        recording_args = ""

        if self.stream.recording_file is not None:
            video_split = "split=3 [vtemp001][vtemp002][vout003]"
            recording_args += f"-map [vout003] -c:v:2 copy {self.stream.recording_file}.mp4"

        args = f"-y -listen 1 -hide_banner -i {url} " \
               f"-filter_complex \"[v:0] {video_split};" \
               f"[vtemp001]scale=w='min(1920\, iw*3/2):h=-1'[vout001];" \
               f"[vtemp002]scale=w='min(1280\, iw*3/2):h=-1'[vout002]\" " \
               f"-preset veryfast -g 30 -sc_threshold 0 " \
               f"-map a:0 -map a:0 -c:a aac -b:a 128k -ac 2 " \
               f"-map [vout001] -c:v:0 libx264 -b:v:0 6000k -maxrate:v:0 6600k -bufsize:v:0 8000k " \
               f"-map [vout002] -c:v:1 libx264 -b:v:1 1000k -maxrate:v:1 1100k -bufsize:v:1 2000k " \
               f"-f hls -hls_time 2 -hls_list_size 2 -hls_flags independent_segments " \
               f"-master_pl_name stream.m3u8 -hls_segment_filename stream_%v/data%06d.ts -use_localtime_mkdir 1 " \
               f"-var_stream_map \"v:0,a:0 v:1,a:1\" stream_%v.m3u8 "

        args += recording_args

        if self.user:
            program = "sudo"
            args = f"-u {self.user} {encoder_settings.binary_ffmpeg}" + args

        self.ffmpeg_process = await asyncio.create_subprocess_exec(program, *shlex.split(args),
                                                                   stdout=asyncio.subprocess.PIPE,
                                                                   stderr=asyncio.subprocess.STDOUT,
                                                                   cwd=self.stream.dir)

        self.watch_task = asyncio.create_task(self.watch_ffmpeg())

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

    async def watch_ffmpeg(self):
        try:
            if self.ffmpeg_process is None:
                logger.warning("started watch of ffmpeg process with no process alive")
                return
            else:
                logger.info(f"started watch for stream {self.stream.stream_id}")
            buffer_counter = 0
            expected_states = [StreamState.NOT_YET_STARTED, StreamState.WAITING_FOR_CONNECTION, StreamState.BUFFERING,
                               StreamState.STREAMING]
            if self.stream.state not in expected_states:
                logger.error(f"stream {self.stream.stream_id} in unexpected state ({self.stream.state})")
            while self.stream.state in expected_states:
                if self.stream.state == StreamState.NOT_YET_STARTED:
                    self.stream.state = StreamState.WAITING_FOR_CONNECTION
                    logger.info(f"watch <stream {self.stream.stream_id}>: state changed: wait for connection")
                # first ffmpeg output will come when stream started
                line = await self.ffmpeg_process.stdout.readline()
                line_str = line.decode('utf-8').rstrip()
                logger.debug(line_str)
                if self.stream.state == StreamState.WAITING_FOR_CONNECTION:
                    self.stream.state = StreamState.BUFFERING
                    logger.info(f"watch <stream {self.stream.stream_id}>: received data; state changed: buffering")
                if self.stream.state == StreamState.BUFFERING:
                    if re.match(r"\[.*\] Unexpected stream", line_str):
                        # handle this case for security reasons
                        logger.error(f"unexpected stream, stop stream {self.stream.stream_id} immediately")
                        self.stream.state = StreamState.UNEXPECTED_STREAM
                        self.ffmpeg_process.kill()
                    if re.match(r"\[.*\] Opening '.+\.tmp' for writing", line_str):
                        buffer_counter += 1
                        logger.debug(f"watch <stream {self.stream.stream_id}>: buffer_counter: {buffer_counter}")
                        if buffer_counter >= 3:
                            self.stream.state = StreamState.STREAMING
                            logger.info(f"watch <stream {self.stream.stream_id}>: state changed: streaming")
                if self.ffmpeg_process.stdout.at_eof():
                    await self.wait()
                    self.stream.state = StreamState.STOPPED
                    logger.info(f"watch <stream {self.stream.stream_id}>: state changed: stopped")
            logger.info(f"watch for stream {self.stream.stream_id} ended")
        except asyncio.CancelledError:
            logger.warning(f"watch for stream {self.stream.stream_id} was CANCELLED unexpectedly")
            self.watch_task = None

    async def wait(self):
        # never wait for watch_task since it waits itself
        await self.ffmpeg_process.wait()
        await self.socat_process.wait()

    async def stop(self, kill_in: float = 2.0):
        for process_name in ['ffmpeg_process', 'socat_process']:
            process = getattr(self, process_name, None)
            if process:
                if kill_in > 0:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), kill_in)
                    except asyncio.TimeoutError:
                        process.kill()
                else:
                    process.kill()
            setattr(self, process_name, None)
        if self.watch_task:
            self.watch_task.cancel()

    async def kill(self):
        await self.stop(kill_in=0)


class EncoderStream(Stream):
    def __init__(self, stream_id: int, ip_range: Optional[str] = None):
        super().__init__(stream_id, ip_range, logger)
        self.port: int = get_unused_port(port_type=PortType.INTERNAL)
        self.public_port: int = get_unused_port(port_type=PortType.PUBLIC)
        self.control_task: Optional[asyncio.Task] = None
        self.watch_task: Optional[asyncio.Task] = None
        self.ffmpeg: Optional[FFmpeg] = None
        self.dir: Optional[Path] = None
        self.recording_file: Optional[Path] = None
        self.state = StreamState.NOT_YET_STARTED

    def start(self):
        # Run everything in an own task.
        self.control_task = asyncio.create_task(self.control())

    async def control(self):
        try:
            await self.create_temp_path()
            logger.info(f"Start ffmpeg on port {self.port} for stream id {self.stream_id}")

            # Start ffmpeg.
            self.state = StreamState.NOT_YET_STARTED
            self.ffmpeg = FFmpeg(self)
            await self.ffmpeg.start()
            await self.ffmpeg.wait()
            logger.info(f"ffmpeg on port {self.port} for stream id {self.stream_id} ended")
            await asyncio.sleep(5)
            if self.watch_task and (not self.watch_task.done() or not self.watch_task.cancelled()):
                logger.warning(f"ffmpeg watch still running for stream id {self.stream_id} - cancelling task")
                self.watch_task.cancel()

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

        It also creates all parent dirs if needed and a temporary file for the recording."""
        if self.dir:
            # As long as this exists, we assume the dir exists in the system.
            return

        self.dir = Path(encoder_settings.hls_temp_dir, str(self.stream_id))
        # there might be blocking io, run in another thread
        await asyncio.get_event_loop().run_in_executor(None,
                functools.partial(self.dir.mkdir, parents=True, exist_ok=True))

        rec_name = "recording_" + ''.join(random.choices(string.ascii_lowercase, k=8))
        self.recording_file = Path(self.dir, rec_name)

    def _get_url(self, port: PortType):
        return f"rtmp://localhost:{self.public_port if port is PortType.PUBLIC else self.port}/stream"

    def get_url(self):
        return self._get_url(PortType.INTERNAL)

    def get_public_url(self):
        return self._get_url(PortType.PUBLIC)

    async def start_recording(self, info: StreamRecordingStartParams):
        # save current position in video (current video duration or better current file size?)
        raise NotImplementedError()

    async def stop_recording(self, withdraw_recording: bool):
        # save current position in video (current video duration or better current file size?)
        # Be prepared that the teacher may start another recording after stopping the current.
        raise NotImplementedError()


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
